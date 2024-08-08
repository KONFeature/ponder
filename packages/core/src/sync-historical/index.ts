import type { Common } from "@/common/common.js";
import { getHistoricalSyncProgress } from "@/common/metrics.js";
import type { Network } from "@/config/networks.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  type BlockFilter,
  type CallTraceFilter,
  type Factory,
  type Filter,
  type LogFactory,
  type LogFilter,
  isAddressFactory,
} from "@/sync/source.js";
import type { Source } from "@/sync/source.js";
import type { SyncBlock, SyncCallTrace, SyncLog } from "@/types/sync.js";
import { formatEta, formatPercentage } from "@/utils/format.js";
import {
  type Interval,
  getChunks,
  intervalDifference,
  intervalSum,
} from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import {
  _eth_getBlockByHash,
  _eth_getBlockByNumber,
  _eth_getLogs,
  _eth_getTransactionReceipt,
  _trace_filter,
} from "@/utils/rpc.js";
import { dedupe } from "@ponder/common";
import { type Address, type Hash, hexToBigInt, hexToNumber, toHex } from "viem";

export type HistoricalSync = {
  /** Closest-to-tip block that is synced. */
  latestBlock: SyncBlock | undefined;
  /** Extract raw data for `interval`. */
  sync(interval: Interval): Promise<void>;
  initializeMetrics(finalizedBlock: SyncBlock): void;
  kill(): void;
};

type CreateHistoricaSyncParameters = {
  common: Common;
  sources: Source[];
  syncStore: SyncStore;
  network: Network;
  requestQueue: RequestQueue;
};

const ADDRESS_FILTER_LIMIT = 1_000;

export const createHistoricalSync = async (
  args: CreateHistoricaSyncParameters,
): Promise<HistoricalSync> => {
  /**
   * Blocks that have already been extracted.
   * Note: All entries are deleted at the end of each call to `sync()`.
   */
  const blockCache = new Map<bigint, Promise<SyncBlock>>();

  // const logMetadata = new Map<LogFilter, { range: number }>();

  /** Intervals that have been completed for all "event" and "address" filters
   * in `args.sources`.
   *
   * Note: `intervalsCache` is not updated after a new interval is synced.
   */
  const intervalsCache: Map<Filter | Factory, Interval[]> = new Map();

  // Populate `intervalsCache` by querying the sync-store.
  for (const { filter } of args.sources) {
    const intervals = await args.syncStore.getIntervals({ filter });
    intervalsCache.set(filter, intervals);
  }

  // Closest-to-tip block that has been fully injested.
  let latestBlock: SyncBlock | undefined;

  ////////
  // Helper functions for specific sync tasks
  ////////

  const syncLogFilter = async (filter: LogFilter, interval: Interval) => {
    // Request last block of interval
    const blockPromise = _eth_getBlockByNumber(args.requestQueue, {
      blockNumber: interval[1],
    });

    // Resolve `filter.address`
    let address: Address | Address[] | undefined;
    if (isAddressFactory(filter.address)) {
      const _addresses = await syncAddress(filter.address, interval);
      if (_addresses.length < ADDRESS_FILTER_LIMIT) {
        address = _addresses;
      } else {
        address = undefined;
      }
    } else {
      address = filter.address;
    }

    // Request logs, batching of large arrays of addresses
    let logsPromise: Promise<SyncLog[]>;
    if (Array.isArray(address) && address.length > 50) {
      const _promises: Promise<SyncLog[]>[] = [];
      for (let i = 0; i < address.length; i += 50) {
        _promises.push(
          _eth_getLogs(args.requestQueue, {
            address: address.slice(i, i + 50),
            topics: filter.topics,
            fromBlock: interval[0],
            toBlock: interval[1],
          }),
        );
      }
      logsPromise = Promise.all(_promises).then((logs) => logs.flat());
    } else {
      logsPromise = _eth_getLogs(args.requestQueue, {
        address,
        topics: filter.topics,
        fromBlock: interval[0],
        toBlock: interval[1],
      });
    }

    const [logs] = await Promise.all([logsPromise, blockPromise]);

    const transactionHashes = new Set(logs.map((l) => l.transactionHash));
    const blocks = await Promise.all(
      logs.map((log) =>
        syncBlock(hexToBigInt(log.blockNumber), transactionHashes),
      ),
    );

    if (logs.length !== 0) {
      await args.syncStore.insertLogs({
        logs: logs.map((log, i) => ({ log, block: blocks[i]! })),
        chainId: args.network.chainId,
      });
    }
  };

  const syncBlockFilter = async (filter: BlockFilter, interval: Interval) => {
    const baseOffset = (interval[0] - filter.offset) % filter.interval;
    const offset = baseOffset === 0 ? 0 : filter.interval - baseOffset;

    // Determine which blocks are matched by the block filter.
    const requiredBlocks: number[] = [];
    for (let b = interval[0] + offset; b <= interval[1]; b += filter.interval) {
      requiredBlocks.push(b);
    }

    await Promise.all(requiredBlocks.map((b) => syncBlock(BigInt(b))));
  };

  const syncTraceFilter = async (
    filter: CallTraceFilter,
    interval: Interval,
  ) => {
    // Request last block of interval
    const blockPromise = _eth_getBlockByNumber(args.requestQueue, {
      blockNumber: interval[1],
    });

    // Resolve `filter.toAddress`
    let toAddress: Address[] | undefined;
    if (isAddressFactory(filter.toAddress)) {
      const _addresses = await syncAddress(filter.toAddress, interval);
      if (_addresses.length < ADDRESS_FILTER_LIMIT) {
        toAddress = _addresses;
      } else {
        toAddress = undefined;
      }
    } else {
      toAddress = filter.toAddress;
    }

    let callTraces = await Promise.all(
      getChunks({ interval, maxChunkSize: 10 }).map((interval) =>
        _trace_filter(args.requestQueue, {
          fromAddress: filter.fromAddress,
          toAddress,
          fromBlock: interval[0],
          toBlock: interval[1],
        }),
      ),
    ).then(
      (traces) =>
        traces.flat().filter((t) => t.type === "call") as SyncCallTrace[],
    );

    await blockPromise;

    // Request transactionReceipts to check for reverted transactions.
    const transactionReceipts = await Promise.all(
      dedupe(callTraces.map((t) => t.transactionHash)).map((hash) =>
        _eth_getTransactionReceipt(args.requestQueue, {
          hash,
        }),
      ),
    );

    const revertedTransactions = new Set<Hash>();
    for (const receipt of transactionReceipts) {
      if (receipt.status === "0x0") {
        revertedTransactions.add(receipt.transactionHash);
      }
    }

    callTraces = callTraces.filter(
      (trace) => revertedTransactions.has(trace.transactionHash) === false,
    );

    const transactionHashes = new Set(callTraces.map((t) => t.transactionHash));
    const blocks = await Promise.all(
      callTraces.map((trace) =>
        syncBlock(hexToBigInt(trace.blockNumber), transactionHashes),
      ),
    );

    if (callTraces.length !== 0) {
      await args.syncStore.insertCallTraces({
        callTraces: callTraces.map((callTrace, i) => ({
          callTrace,
          block: blocks[i]!,
        })),
        chainId: args.network.chainId,
      });
    }
  };

  /** Extract and insert the log-based addresses that match `filter` + `interval`. */
  const syncLogAddressFilter = async (
    filter: LogFactory,
    interval: Interval,
  ) => {
    const logs = await _eth_getLogs(args.requestQueue, {
      address: filter.address,
      topics: [filter.eventSelector],
      fromBlock: interval[0],
      toBlock: interval[1],
    });

    // Insert `logs` into the sync-store
    if (logs.length !== 0) {
      await args.syncStore.insertLogs({
        logs: logs.map((log) => ({ log })),
        chainId: args.network.chainId,
      });
    }
  };

  /**
   * Extract block, using `blockCache` to avoid fetching
   * the same block twice. Also, update `latestBlock`.
   *
   * @param number Block to be extracted
   * @param transactionHashes Hashes to be inserted into the sync-store
   *
   * Note: This function could more accurately skip network requests by taking
   * advantage of `syncStore.hasBlock` and `syncStore.hasTransaction`.
   */
  const syncBlock = async (
    number: bigint,
    transactionHashes?: Set<Hash>,
  ): Promise<SyncBlock> => {
    let block: SyncBlock;

    /**
     * `blockCache` contains all blocks that have been extracted during the
     * current call to `sync()`. If `number` is present in `blockCache` use it,
     * otherwise, request the block and add it to `blockCache` and the sync-store.
     */

    if (blockCache.has(number)) {
      block = await blockCache.get(number)!;
    } else {
      const _block = _eth_getBlockByNumber(args.requestQueue, {
        blockNumber: toHex(number),
      });
      blockCache.set(number, _block);
      block = await _block;
      await args.syncStore.insertBlock({
        block,
        chainId: args.network.chainId,
      });

      // Update `latestBlock` if `block` is closer to tip.
      if (
        hexToBigInt(block.number) > hexToBigInt(latestBlock?.number ?? "0x0")
      ) {
        latestBlock = block;
      }
    }

    // Add `transactionHashes` to the sync-store.
    if (
      transactionHashes !== undefined &&
      transactionHashes.size > 0 &&
      block.transactions.length > 0
    ) {
      await args.syncStore.insertTransactions({
        transactions: block.transactions.filter((transaction) =>
          transactionHashes.has(transaction.hash),
        ),
        chainId: args.network.chainId,
      });
    }

    return block;
  };

  /**
   * Return all addresses that match `filter` after extracting addresses
   * that match `filter` and `interval`.
   */
  const syncAddress = async (
    filter: Factory,
    interval: Interval,
  ): Promise<Address[]> => {
    await syncLogAddressFilter(filter, interval);

    // TODO(kyle) should "address" metrics be tracked?

    // Query the sync-store for all addresses that match `filter`.
    return await args.syncStore.getChildAddresses({
      filter,
      limit: ADDRESS_FILTER_LIMIT,
    });
  };

  // TODO(kyle) use filter metadata for recommended "eth_getLogs" ranges

  // Emit progress update logs on an interval for each source.
  const interval = setInterval(async () => {
    const historical = await getHistoricalSyncProgress(args.common.metrics);

    for (const {
      networkName,
      sourceName,
      progress,
      eta,
    } of historical.sources) {
      // TODO(kyle) check `requiredIntervals`.
      if (progress === 1 || networkName !== args.network.name) return;
      args.common.logger.info({
        service: "historical",
        msg: `Syncing '${networkName}' for '${sourceName}' with ${formatPercentage(
          progress ?? 0,
        )} complete${eta !== undefined ? ` and ~${formatEta(eta)} remaining` : ""}`,
      });
    }
  }, 10_000);

  return {
    get latestBlock() {
      return latestBlock;
    },
    async sync(_interval) {
      await Promise.all(
        args.sources.map(async ({ filter, ...source }) => {
          // Compute the required interval to sync, accounting for cached
          // intervals and start + end block.

          // Skip sync if the interval is after the `toBlock`.
          if (filter.toBlock && filter.toBlock < _interval[0]) return;
          const interval: Interval = [
            Math.max(filter.fromBlock, _interval[0]),
            Math.min(filter.toBlock ?? Number.POSITIVE_INFINITY, _interval[1]),
          ];
          const completedIntervals = intervalsCache.get(filter)!;
          const requiredIntervals = intervalDifference(
            [interval],
            completedIntervals,
          );

          // Skip sync if the interval is already complete.
          if (requiredIntervals.length === 0) return;

          // sync required intervals
          await Promise.all(
            requiredIntervals.map(async (interval) => {
              switch (filter.type) {
                case "log":
                  await syncLogFilter(filter, interval);
                  break;

                case "block":
                  await syncBlockFilter(filter, interval);
                  break;

                case "callTrace":
                  await syncTraceFilter(filter, interval);
                  break;

                default:
                  never(filter);
              }
            }),
          );

          // Mark `interval` for `filter` as completed in the sync-store
          await args.syncStore.insertInterval({ filter, interval });

          args.common.metrics.ponder_historical_completed_blocks.inc(
            {
              network: source.networkName,
              source: source.name,
              type: filter.type,
            },
            interval[1] - interval[0] + 1,
          );
        }),
      );
      blockCache.clear();
    },
    initializeMetrics(finalizedBlock) {
      for (const source of args.sources) {
        const label = {
          network: source.networkName,
          source: source.name,
          type: source.filter.type,
        };

        if (source.filter.fromBlock > hexToNumber(finalizedBlock.number)) {
          args.common.metrics.ponder_historical_total_blocks.set(label, 0);

          args.common.logger.warn({
            service: "historical",
            msg: `Skipped syncing '${source.networkName}' for '${source.name}' because the start block is not finalized`,
          });
        } else {
          const interval = [
            source.filter.fromBlock,
            source.filter.toBlock ?? hexToNumber(finalizedBlock.number),
          ] satisfies Interval;

          const requiredIntervals = intervalDifference(
            [interval],
            intervalsCache.get(source.filter)!,
          );

          const totalBlocks = interval[1] - interval[0] + 1;
          const cachedBlocks = totalBlocks - intervalSum(requiredIntervals);

          args.common.metrics.ponder_historical_total_blocks.set(
            label,
            totalBlocks,
          );

          args.common.metrics.ponder_historical_cached_blocks.set(
            label,
            cachedBlocks,
          );

          // TODO(kyle) different message for full cache?

          args.common.logger.info({
            service: "historical",
            msg: `Started syncing '${source.networkName}' for '${
              source.name
            }' with ${formatPercentage(
              Math.min(1, cachedBlocks / (totalBlocks || 1)),
            )} cached`,
          });
        }
      }
    },
    kill() {
      clearInterval(interval);
    },
  };
};
