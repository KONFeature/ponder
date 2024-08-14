import type { Common } from "@/common/common.js";
import { NonRetryableError } from "@/common/errors.js";
import type { HeadlessKysely } from "@/database/kysely.js";
import type { RawEvent } from "@/sync/events.js";
import {
  type BlockFilterFragment,
  type LogFilterFragment,
  type TraceFilterFragment,
  buildBlockFilterFragment,
  buildLogFilterFragments,
  buildTraceFilterFragments,
} from "@/sync/fragments.js";
import {
  type BlockFilter,
  type CallTraceFilter,
  type Factory,
  type Filter,
  type LogFactory,
  type LogFilter,
  isAddressFactory,
} from "@/sync/source.js";
import type { Block, Log, Transaction } from "@/types/eth.js";
import type {
  SyncBlock,
  SyncCallTrace,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/types/sync.js";
import type { NonNull } from "@/types/utils.js";
import { EVENT_TYPES, encodeCheckpoint } from "@/utils/checkpoint.js";
import { decodeToBigInt } from "@/utils/encoding.js";
import {
  type Interval,
  intervalIntersectionMany,
  intervalUnion,
} from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import { startClock } from "@/utils/timer.js";
import { type Kysely, type SelectQueryBuilder, sql as ksql } from "kysely";
import {
  type Address,
  type Hash,
  type Hex,
  checksumAddress,
  hexToBigInt,
  hexToNumber,
} from "viem";
import {
  type PonderSyncSchema,
  encodeBlock,
  encodeCallTrace,
  encodeLog,
  encodeTransaction,
  encodeTransactionReceipt,
  formatBig,
  parseBig,
} from "./encoding.js";

export type SyncStore = {
  insertInterval(args: {
    filter: Filter;
    interval: Interval;
  }): Promise<void>;
  getIntervals(args: {
    filter: Filter;
  }): Promise<Interval[]>;
  getChildAddresses(args: {
    filter: Factory;
    limit: number;
  }): Promise<Address[]>;
  filterChildAddresses(args: {
    filter: Factory;
    addresses: Address[];
  }): Promise<Set<Address>>;
  insertLogs(args: {
    logs: { log: SyncLog; block?: SyncBlock }[];
    chainId: number;
  }): Promise<void>;
  insertBlock(args: { block: SyncBlock; chainId: number }): Promise<void>;
  /** Return true if the block receipt is present in the database. */
  hasBlock(args: { hash: Hash }): Promise<boolean>;
  insertTransactions(args: {
    transactions: SyncTransaction[];
    chainId: number;
  }): Promise<void>;
  /** Return true if the transaction is present in the database. */
  hasTransaction(args: { hash: Hash }): Promise<boolean>;
  insertTransactionReceipts(args: {
    transactionReceipts: SyncTransactionReceipt[];
    chainId: number;
  }): Promise<void>;
  /** Return true if the transaction receipt is present in the database. */
  hasTransactionReceipt(args: { hash: Hash }): Promise<boolean>;
  insertCallTraces(args: {
    callTraces: { callTrace: SyncCallTrace; block: SyncBlock }[];
    chainId: number;
  }): Promise<void>;
  /** Returns an ordered list of events based on the `filters` and pagination arguments. */
  getEvents(args: {
    filters: Filter[];
    from: string;
    to: string;
    limit: number;
  }): Promise<{ events: RawEvent[]; cursor: string }>;
  insertRpcRequestResult(args: {
    request: string;
    blockNumber: bigint;
    chainId: number;
    result: string;
  }): Promise<void>;
  getRpcRequestResult(args: {
    request: string;
    blockNumber: bigint;
    chainId: number;
  }): Promise<string | null>;
  pruneByBlock(args: {
    fromBlock: number;
    chainId: number;
  }): Promise<void>;
  pruneByChain(args: {
    fromBlock: number;
    chainId: number;
  }): Promise<void>;
};

const childAddressSQL = (
  sql: "sqlite" | "postgres",
  childAddressLocation: LogFactory["childAddressLocation"],
) => {
  if (childAddressLocation.startsWith("offset")) {
    const childAddressOffset = Number(childAddressLocation.substring(6));
    const start = 2 + 12 * 2 + childAddressOffset * 2 + 1;
    const length = 20 * 2;
    return sql === "sqlite"
      ? ksql<Hex>`'0x' || substring(data, ${start}, ${length})`
      : ksql<Hex>`'0x' || substring(data from ${start}::int for ${length}::int)`;
  } else {
    const start = 2 + 12 * 2 + 1;
    const length = 20 * 2;
    return sql === "sqlite"
      ? ksql<Hex>`'0x' || substring(${ksql.ref(childAddressLocation)}, ${start}, ${length})`
      : ksql<Hex>`'0x' || substring(${ksql.ref(
          childAddressLocation,
        )} from ${start}::integer for ${length}::integer)`;
  }
};

export const createSyncStore = ({
  common,
  db,
  sql,
}: {
  common: Common;
  sql: "sqlite" | "postgres";
  db: HeadlessKysely<PonderSyncSchema>;
}): SyncStore => ({
  insertInterval: async ({ filter, interval }) =>
    db.wrap({ method: "insertInterval" }, async () => {
      const intervalToBlock = (interval: Interval) => ({
        startBlock: formatBig(sql, interval[0]),
        endBlock: formatBig(sql, interval[1]),
      });

      switch (filter.type) {
        case "log":
          {
            await db.transaction().execute(async (tx) => {
              for (const fragment of buildLogFilterFragments(filter)) {
                if (isAddressFactory(filter.address)) {
                  await tx
                    .insertInto("factoryLogFilters")
                    .values(fragment as LogFilterFragment<Factory>)
                    .onConflict((oc) => oc.column("id").doNothing())
                    .execute();

                  await tx
                    .insertInto("factoryLogFilterIntervals")
                    .values({
                      factoryId: fragment.id,
                      ...intervalToBlock(interval),
                    })
                    .execute();
                } else {
                  await tx
                    .insertInto("logFilters")
                    .values(fragment)
                    .onConflict((oc) => oc.column("id").doNothing())
                    .execute();

                  await tx
                    .insertInto("logFilterIntervals")
                    .values({
                      logFilterId: fragment.id,
                      ...intervalToBlock(interval),
                    })
                    .execute();
                }
              }
            });
          }
          break;

        case "block":
          {
            const fragment = buildBlockFilterFragment(filter);
            await db.transaction().execute(async (tx) => {
              await tx
                .insertInto("blockFilters")
                .values(fragment)
                .onConflict((oc) => oc.column("id").doNothing())
                .executeTakeFirstOrThrow();

              await tx
                .insertInto("blockFilterIntervals")
                .values({
                  blockFilterId: fragment.id,
                  ...intervalToBlock(interval),
                })
                .execute();
            });
          }
          break;

        case "callTrace":
          {
            await db.transaction().execute(async (tx) => {
              for (const fragment of buildTraceFilterFragments(filter)) {
                if (isAddressFactory(filter.toAddress)) {
                  await tx
                    .insertInto("factoryTraceFilters")
                    .values(fragment as TraceFilterFragment<Factory>)
                    .onConflict((oc) => oc.column("id").doNothing())
                    .execute();

                  await tx
                    .insertInto("factoryTraceFilterIntervals")
                    .values({
                      factoryId: fragment.id,
                      ...intervalToBlock(interval),
                    })
                    .execute();
                } else {
                  await tx
                    .insertInto("traceFilters")
                    .values(fragment)
                    .onConflict((oc) => oc.column("id").doNothing())
                    .execute();

                  await tx
                    .insertInto("traceFilterIntervals")
                    .values({
                      traceFilterId: fragment.id,
                      ...intervalToBlock(interval),
                    })
                    .execute();
                }
              }
            });
          }
          break;

        default:
          never(filter);
      }
    }),
  getIntervals: async ({ filter }) =>
    db.wrap({ method: "getIntervals" }, async () => {
      const topicSQL = (
        qb: SelectQueryBuilder<
          PonderSyncSchema,
          | "logFilters"
          | "logFilterIntervals"
          | "factoryLogFilters"
          | "factoryLogFilterIntervals",
          {}
        >,
        fragment: LogFilterFragment,
      ) =>
        qb
          .where((eb) =>
            eb.or([
              eb("topic0", "is", null),
              eb("topic0", "=", fragment.topic0),
            ]),
          )
          .where((eb) =>
            eb.or([
              eb("topic1", "is", null),
              eb("topic1", "=", fragment.topic1),
            ]),
          )
          .where((eb) =>
            eb.or([
              eb("topic2", "is", null),
              eb("topic2", "=", fragment.topic2),
            ]),
          )
          .where((eb) =>
            eb.or([
              eb("topic3", "is", null),
              eb("topic3", "=", fragment.topic3),
            ]),
          );

      let fragments:
        | LogFilterFragment[]
        | TraceFilterFragment[]
        | BlockFilterFragment[];
      let table:
        | "logFilter"
        | "factoryLogFilter"
        | "traceFilter"
        | "factoryTraceFilter"
        | "blockFilter";
      let idCol:
        | "logFilterId"
        | "traceFilterId"
        | "blockFilterId"
        | "factoryId";
      let fragmentSelect: (
        fragment: any,
        qb: SelectQueryBuilder<PonderSyncSchema, keyof PonderSyncSchema, {}>,
      ) => SelectQueryBuilder<PonderSyncSchema, keyof PonderSyncSchema, {}>;

      switch (filter.type) {
        case "log":
          {
            if (isAddressFactory(filter.address)) {
              fragments = buildLogFilterFragments(filter);
              table = "factoryLogFilter";
              idCol = "factoryId";
              // @ts-ignore
              fragmentSelect = (fragment: LogFilterFragment<LogFactory>, qb) =>
                qb
                  .where("address", "=", fragment.address)
                  .where("eventSelector", "=", fragment.eventSelector)
                  .where(
                    "childAddressLocation",
                    "=",
                    fragment.childAddressLocation,
                  )
                  .where(
                    "includeTransactionReceipts",
                    ">=",
                    fragment.includeTransactionReceipts,
                  )
                  .$call((qb) => topicSQL(qb, fragment));
            } else {
              fragments = buildLogFilterFragments(filter);
              table = "logFilter";
              idCol = "logFilterId";
              // @ts-ignore
              fragmentSelect = (fragment: LogFilterFragment<undefined>, qb) =>
                qb
                  .where((eb) =>
                    eb.or([
                      eb("address", "is", null),
                      eb("address", "=", fragment.address),
                    ]),
                  )
                  .where(
                    "includeTransactionReceipts",
                    ">=",
                    fragment.includeTransactionReceipts,
                  )
                  .$call((qb) => topicSQL(qb, fragment));
            }
          }
          break;

        case "block":
          {
            fragments = [buildBlockFilterFragment(filter)];
            table = "blockFilter";
            idCol = "blockFilterId";
            fragmentSelect = (fragment, qb) =>
              qb.where("blockFilterId", "=", fragment.id);
          }
          break;

        case "callTrace":
          {
            if (isAddressFactory(filter.toAddress)) {
              fragments = buildTraceFilterFragments(filter);
              table = "factoryTraceFilter";
              idCol = "factoryId";
              fragmentSelect = (fragment: TraceFilterFragment<Factory>, qb) =>
                qb
                  .where("address", "=", fragment.address)
                  .where("eventSelector", "=", fragment.eventSelector)
                  .where(
                    "childAddressLocation",
                    "=",
                    fragment.childAddressLocation,
                  )
                  .where((eb) =>
                    eb.or([
                      eb("fromAddress", "is", null),
                      eb("fromAddress", "=", fragment.fromAddress),
                    ]),
                  );
            } else {
              fragments = buildTraceFilterFragments(filter);
              table = "traceFilter";
              idCol = "traceFilterId";
              fragmentSelect = (fragment: TraceFilterFragment<undefined>, qb) =>
                qb
                  .where((eb) =>
                    eb.or([
                      eb("fromAddress", "is", null),
                      eb("fromAddress", "=", fragment.fromAddress),
                    ]),
                  )
                  .where((eb) =>
                    eb.or([
                      eb("toAddress", "is", null),
                      eb("toAddress", "=", fragment.toAddress),
                    ]),
                  );
            }
          }
          break;

        default:
          never(filter);
      }

      // First, attempt to merge overlapping and adjacent intervals.
      for (const fragment of fragments!) {
        await db.transaction().execute(async (tx) => {
          while (true) {
            await tx
              .insertInto(`${table}s`)
              .values(fragment)
              .onConflict((oc) => oc.column("id").doNothing())
              .executeTakeFirstOrThrow();

            // This is a trick to add a LIMIT to a DELETE statement
            const existingIntervals = await tx
              .deleteFrom(`${table}Intervals`)
              .where(
                "id",
                "in",
                tx
                  .selectFrom(`${table}Intervals`)
                  .where(idCol, "=", fragment.id)
                  .select("id")
                  .limit(common.options.syncStoreMaxIntervals),
              )
              .returning(["startBlock", "endBlock"])
              .execute();

            const mergedIntervals = intervalUnion(
              existingIntervals.map((i) =>
                sql === "sqlite"
                  ? [
                      Number(decodeToBigInt(i.startBlock as string)),
                      Number(decodeToBigInt(i.endBlock as string)),
                    ]
                  : [Number(i.startBlock), Number(i.endBlock)],
              ),
            );

            const mergedIntervalRows = mergedIntervals.map(
              ([startBlock, endBlock]) => ({
                [idCol as string]: fragment.id,
                startBlock: formatBig(sql, startBlock),
                endBlock: formatBig(sql, endBlock),
              }),
            );

            if (mergedIntervalRows.length > 0) {
              await tx
                .insertInto(`${table}Intervals`)
                .values(mergedIntervalRows)
                .execute();
            }

            if (
              mergedIntervalRows.length === common.options.syncStoreMaxIntervals
            ) {
              // This occurs when there are too many non-mergeable ranges with the same logFilterId. Should be almost impossible.
              throw new NonRetryableError(
                `'${table}Intervals' table for chain '${fragment.chainId}' has reached an unrecoverable level of fragmentation.`,
              );
            }

            if (
              existingIntervals.length !== common.options.syncStoreMaxIntervals
            )
              break;
          }
        });
      }

      const intervals: Interval[][] = [];
      for (const fragment of fragments!) {
        const _intervals = await db
          .selectFrom(`${table!}Intervals`)
          .innerJoin(`${table!}s`, idCol!, `${table!}s.id`)
          .$call((qb) => fragmentSelect(fragment, qb as any))
          .where("chainId", "=", fragment.chainId)
          .select(["startBlock", "endBlock"])
          .execute();

        const union = intervalUnion(
          _intervals.map(({ startBlock, endBlock }) =>
            sql === "sqlite"
              ? [
                  Number(decodeToBigInt(startBlock as string)),
                  Number(decodeToBigInt(endBlock as string)),
                ]
              : [Number(startBlock), Number(endBlock)],
          ),
        );

        intervals.push(union);
      }

      return intervalIntersectionMany(intervals);
    }),
  getChildAddresses: ({ filter, limit }) =>
    db.wrap({ method: "getChildAddresses" }, async () => {
      return await db
        .selectFrom("logs")
        .select(childAddressSQL(sql, filter.childAddressLocation).as("address"))
        .where("address", "=", filter.address)
        .where("topic0", "=", filter.eventSelector)
        .where("chainId", "=", filter.chainId)
        .orderBy("id asc")
        .limit(limit)
        .execute()
        .then((addresses) => addresses.map(({ address }) => address));
    }),
  filterChildAddresses: ({ filter, addresses }) =>
    db.wrap({ method: "filterChildAddresses" }, async () => {
      const result = await db
        .with(
          "addresses(address)",
          () =>
            ksql`( values ${ksql.join(addresses.map((a) => ksql`( ${ksql.val(a)} )`))} )`,
        )
        .with("childAddresses", (db) =>
          db
            .selectFrom("logs")
            .select(
              childAddressSQL(sql, filter.childAddressLocation).as("address"),
            )
            .where("address", "=", filter.address)
            .where("topic0", "=", filter.eventSelector)
            .where("chainId", "=", filter.chainId),
        )
        .selectFrom("addresses")
        .where(
          "addresses.address",
          "in",
          ksql`(SELECT "address" FROM "childAddresses")`,
        )
        .selectAll()
        .execute();

      return new Set<Address>([...result.map(({ address }) => address)]);
    }),
  insertLogs: async ({ logs, chainId }) =>
    db.wrap({ method: "insertLogs" }, async () => {
      if (logs.length === 0) return;

      // Calculate `batchSize` based on how many parameters the
      // input will have
      const batchSize = Math.floor(
        common.options.databaseMaxQueryParameters /
          Object.keys(encodeLog({ log: logs[0]!.log, chainId, sql })).length,
      );

      for (let i = 0; i < logs.length; i += batchSize) {
        await db
          .insertInto("logs")
          .values(
            logs
              .slice(i, i + batchSize)
              .map(({ log, block }) => encodeLog({ log, block, chainId, sql })),
          )
          .onConflict((oc) =>
            oc.column("id").doUpdateSet((eb) => ({
              checkpoint: eb.ref("excluded.checkpoint"),
            })),
          )
          .execute();
      }
    }),
  insertBlock: async ({ block, chainId }) =>
    db.wrap({ method: "insertBlock" }, async () => {
      await db
        .insertInto("blocks")
        .values(encodeBlock({ block, chainId, sql }))
        .onConflict((oc) => oc.column("hash").doNothing())
        .execute();
    }),
  hasBlock: async ({ hash }) =>
    db.wrap({ method: "hasBlock" }, async () => {
      return await db
        .selectFrom("blocks")
        .select("hash")
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransactions: async ({ transactions, chainId }) =>
    db.wrap({ method: "insertTransactions" }, async () => {
      if (transactions.length === 0) return;

      // Calculate `batchSize` based on how many parameters the
      // input will have
      const batchSize = Math.floor(
        common.options.databaseMaxQueryParameters /
          Object.keys(
            encodeTransaction({ transaction: transactions[0]!, chainId, sql }),
          ).length,
      );

      for (let i = 0; i < transactions.length; i += batchSize) {
        await db
          .insertInto("transactions")
          .values(
            transactions
              .slice(i, i + batchSize)
              .map((transaction) =>
                encodeTransaction({ transaction, chainId, sql }),
              ),
          )
          .onConflict((oc) =>
            oc.column("hash").doUpdateSet((eb) => ({
              blockHash: eb.ref("excluded.blockHash"),
              blockNumber: eb.ref("excluded.blockNumber"),
              transactionIndex: eb.ref("excluded.transactionIndex"),
            })),
          )
          .execute();
      }
    }),
  hasTransaction: async ({ hash }) =>
    db.wrap({ method: "hasTransaction" }, async () => {
      return await db
        .selectFrom("transactions")
        .select("hash")
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransactionReceipts: async ({ transactionReceipts, chainId }) =>
    db.wrap({ method: "insertTransactionReceipts" }, async () => {
      if (transactionReceipts.length === 0) return;

      // Calculate `batchSize` based on how many parameters the
      // input will have
      const batchSize = Math.floor(
        common.options.databaseMaxQueryParameters /
          Object.keys(
            encodeTransactionReceipt({
              transactionReceipt: transactionReceipts[0]!,
              chainId,
              sql,
            }),
          ).length,
      );

      for (let i = 0; i < transactionReceipts.length; i += batchSize) {
        await db
          .insertInto("transactionReceipts")
          .values(
            transactionReceipts
              .slice(i, i + batchSize)
              .map((transactionReceipt) =>
                encodeTransactionReceipt({ transactionReceipt, chainId, sql }),
              ),
          )
          .onConflict((oc) =>
            oc.column("transactionHash").doUpdateSet((eb) => ({
              blockHash: eb.ref("excluded.blockHash"),
              blockNumber: eb.ref("excluded.blockNumber"),
              contractAddress: eb.ref("excluded.contractAddress"),
              cumulativeGasUsed: eb.ref("excluded.cumulativeGasUsed"),
              effectiveGasPrice: eb.ref("excluded.effectiveGasPrice"),
              gasUsed: eb.ref("excluded.gasUsed"),
              logs: eb.ref("excluded.logs"),
              logsBloom: eb.ref("excluded.logsBloom"),
              transactionIndex: eb.ref("excluded.transactionIndex"),
            })),
          )
          .execute();
      }
    }),
  hasTransactionReceipt: async ({ hash }) =>
    db.wrap({ method: "hasTransactionReceipt" }, async () => {
      return await db
        .selectFrom("transactionReceipts")
        .select("transactionHash")
        .where("transactionHash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertCallTraces: async ({ callTraces, chainId }) =>
    db.wrap({ method: "insertCallTrace" }, async () => {
      if (callTraces.length === 0) return;

      // Delete existing traces with the same `transactionHash`. Then, calculate "callTraces.checkpoint"
      // based on the ordering of "callTraces.traceAddress" and add all traces to "callTraces" table.
      const traceByTransactionHash: {
        [transactionHash: Hex]: { traces: SyncCallTrace[]; block: SyncBlock };
      } = {};

      for (const { callTrace, block } of callTraces) {
        if (traceByTransactionHash[callTrace.transactionHash] === undefined) {
          traceByTransactionHash[callTrace.transactionHash] = {
            traces: [],
            block,
          };
        }
        traceByTransactionHash[callTrace.transactionHash]!.traces.push(
          callTrace,
        );
      }

      const values: PonderSyncSchema["callTraces"][] = [];

      await db.transaction().execute(async (tx) => {
        for (const transactionHash of Object.keys(traceByTransactionHash)) {
          const block = traceByTransactionHash[transactionHash as Hex]!.block;
          const traces = await tx
            .deleteFrom("callTraces")
            .returningAll()
            .where("transactionHash", "=", transactionHash as Hex)
            .where("chainId", "=", chainId)
            .execute();

          traces.push(
            // @ts-ignore
            ...traceByTransactionHash[transactionHash as Hex]!.traces.map(
              (trace) => encodeCallTrace({ trace, chainId, sql }),
            ),
          );

          // Use lexographical sort of stringified `traceAddress`.
          traces.sort((a, b) => {
            return a.traceAddress < b.traceAddress ? -1 : 1;
          });

          for (let i = 0; i < traces.length; i++) {
            const trace = traces[i]!;

            const checkpoint = encodeCheckpoint({
              blockTimestamp: hexToNumber(block.timestamp),
              chainId: BigInt(chainId),
              blockNumber: hexToBigInt(block.number),
              transactionIndex: BigInt(trace.transactionPosition),
              eventType: EVENT_TYPES.callTraces,
              eventIndex: BigInt(i),
            });
            trace.checkpoint = checkpoint;
            values.push(trace);
          }
        }

        // Calculate `batchSize` based on how many parameters the
        // input will have
        const batchSize = Math.floor(
          common.options.databaseMaxQueryParameters /
            Object.keys(values[0]!).length,
        );

        for (let i = 0; i < values.length; i += batchSize) {
          await tx
            .insertInto("callTraces")
            .values(values.slice(i, i + batchSize))
            .onConflict((oc) => oc.column("id").doNothing())
            .execute();
        }
      });
    }),
  getEvents: async ({ filters, from, to, limit }) => {
    const addressSQL = (
      qb: SelectQueryBuilder<
        PonderSyncSchema,
        "logs" | "blocks" | "callTraces",
        {}
      >,
      address: LogFilter["address"],
      column: "address" | "from" | "to",
    ) => {
      if (typeof address === "string") return qb.where(column, "=", address);
      if (Array.isArray(address)) return qb.where(column, "in", address);
      if (isAddressFactory(address)) {
        // log address filter
        return qb.where(
          column,
          "in",
          db
            .selectFrom("logs")
            .select(
              childAddressSQL(sql, address.childAddressLocation).as(
                "childAddress",
              ),
            )
            .where("address", "=", address.address)
            .where("topic0", "=", address.eventSelector)
            .where("chainId", "=", address.chainId),
        );
      }
      return qb;
    };

    const logSQL = (
      filter: LogFilter,
      db: Kysely<PonderSyncSchema>,
      index: number,
    ) =>
      db
        .selectFrom("logs")
        .select([
          ksql.raw(`'${index}'`).as("filterIndex"),
          "checkpoint",
          "chainId",
          "blockHash",
          "transactionHash",
          "id as logId",
          ksql`null`.as("callTraceId"),
        ])
        .where("chainId", "=", filter.chainId)
        .$if(filter.topics !== undefined, (qb) => {
          for (const idx_ of [0, 1, 2, 3]) {
            const idx = idx_ as 0 | 1 | 2 | 3;
            // If it's an array of length 1, collapse it.
            const raw = filter.topics![idx] ?? null;
            if (raw === null) continue;
            const topic =
              Array.isArray(raw) && raw.length === 1 ? raw[0]! : raw;
            if (Array.isArray(topic)) {
              qb = qb.where((eb) =>
                eb.or(topic.map((t) => eb(`logs.topic${idx}`, "=", t))),
              );
            } else {
              qb = qb.where(`logs.topic${idx}`, "=", topic);
            }
          }
          return qb;
        })
        .$call((qb) => addressSQL(qb as any, filter.address, "address"))
        .where("blockNumber", ">=", formatBig(sql, filter.fromBlock))
        .$if(filter.toBlock !== undefined, (qb) =>
          qb.where("blockNumber", "<=", formatBig(sql, filter.toBlock!)),
        );

    const callTraceSQL = (
      filter: CallTraceFilter,
      db: Kysely<PonderSyncSchema>,
      index: number,
    ) =>
      db
        .selectFrom("callTraces")
        .select([
          ksql.raw(`'${index}'`).as("filterIndex"),
          "checkpoint",
          "chainId",
          "blockHash",
          "transactionHash",
          ksql`null`.as("logId"),
          "id as callTraceId",
        ])
        .where((eb) =>
          eb.or(
            filter.functionSelectors.map((fs) =>
              eb("callTraces.functionSelector", "=", fs),
            ),
          ),
        )
        .where(ksql`${ksql.ref("callTraces.error")} IS NULL`)
        .$call((qb) => addressSQL(qb as any, filter.fromAddress, "from"))
        .$call((qb) => addressSQL(qb, filter.toAddress, "to"))
        .where("blockNumber", ">=", formatBig(sql, filter.fromBlock))
        .$if(filter.toBlock !== undefined, (qb) =>
          qb.where("blockNumber", "<=", formatBig(sql, filter.toBlock!)),
        );

    const blockSQL = (
      filter: BlockFilter,
      db: Kysely<PonderSyncSchema>,
      index: number,
    ) =>
      db
        .selectFrom("blocks")
        .select([
          ksql.raw(`'${index}'`).as("filterIndex"),
          "checkpoint",
          "chainId",
          "hash as blockHash",
          ksql`null`.as("transactionHash"),
          ksql`null`.as("logId"),
          ksql`null`.as("callTraceId"),
        ])
        .where("chainId", "=", filter.chainId)
        .$if(filter !== undefined && filter.interval !== undefined, (qb) =>
          qb.where(ksql`(number - ${filter.offset}) % ${filter.interval} = 0`),
        )
        .where("number", ">=", formatBig(sql, filter.fromBlock))
        .$if(filter.toBlock !== undefined, (qb) =>
          qb.where("number", "<=", formatBig(sql, filter.toBlock!)),
        );

    const rows = await db.wrap({ method: "getEvents" }, async () => {
      let query:
        | SelectQueryBuilder<
            PonderSyncSchema,
            "logs" | "callTraces" | "blocks",
            {
              filterIndex: number;
              checkpoint: string;
              chainId: number;
              blockHash: string;
              transactionHash: string;
              logId: string;
              callTraceId: string;
            }
          >
        | undefined;

      for (let i = 0; i < filters.length; i++) {
        const filter = filters[i]!;

        const _query =
          filter.type === "log"
            ? logSQL(filter, db, i)
            : filter.type === "callTrace"
              ? callTraceSQL(filter, db, i)
              : blockSQL(filter, db, i);

        if (query === undefined) {
          // @ts-ignore
          query = _query;
        } else {
          // @ts-ignore
          query = query.unionAll(_query);
        }
      }

      return await db
        .with("event", () => query!)
        .selectFrom("event")
        .innerJoin("blocks", "blocks.hash", "event.blockHash")
        .leftJoin("logs", "logs.id", "event.logId")
        .leftJoin("transactions", "transactions.hash", "event.transactionHash")
        .leftJoin("callTraces", "callTraces.id", "event.callTraceId")
        .leftJoin(
          "transactionReceipts",
          "transactionReceipts.transactionHash",
          "event.transactionHash",
        )
        .selectAll()
        .select([
          "event.filterIndex as event_filterIndex",
          "event.checkpoint as event_checkpoint",
          "transactions.hash as tx_hash",
          "blocks.hash as block_hash",
          "transactionReceipts.transactionHash as txr_hash",
          "blocks.nonce as block_nonce",
          "transactions.nonce as tx_nonce",
        ])
        .where("event.checkpoint", ">", from)
        .where("event.checkpoint", "<=", to)
        .orderBy("event.checkpoint", "asc")
        .orderBy("event.filterIndex", "asc")
        .limit(limit)
        .execute();
    });

    const endClock = startClock();

    const events = rows.map((_row) => {
      // Without this cast, the block_ and tx_ fields are all nullable
      // which makes this very annoying. Should probably add a runtime check
      // that those fields are indeed present before continuing here.
      const row = _row as NonNull<(typeof rows)[number]>;

      const filter = filters[row.event_filterIndex]!;

      const hasLog = row.logId !== null;
      const hasTransaction = row.tx_hash !== null;
      const hasCallTrace = row.callTraceId !== null;
      const hasTransactionReceipt = row.txr_hash !== null;

      const block: Partial<Block> = {
        hash: row.block_hash,
        number: parseBig(sql, row.number),
        timestamp: parseBig(sql, row.timestamp),
      };

      const blockHandler: ProxyHandler<Block> = {
        get: (target, prop, receiver) => {
          if (prop === "baseFeePerGas")
            return row.baseFeePerGas ? parseBig(sql, row.baseFeePerGas) : null;
          if (prop === "difficulty") parseBig(sql, row.difficulty);
          if (prop === "extraData") return row.extraData;
          if (prop === "gasLimit") parseBig(sql, row.gasLimit);
          if (prop === "gasUsed") parseBig(sql, row.gasUsed);

          if (prop === "hash") return row.hash;
          if (prop === "logsBloom") return row.logsBloom;
          if (prop === "miner") checksumAddress(row.miner);
          if (prop === "mixHash") return row.mixHash;
          if (prop === "nonce") return row.block_nonce;
          if (prop === "parentHash") return row.parentHash;
          if (prop === "receiptsRoot") return row.receiptsRoot;
          if (prop === "sha3Uncles") return row.sha3Uncles;
          if (prop === "size") parseBig(sql, row.size);
          if (prop === "stateRoot") return row.stateRoot;
          if (prop === "totalDifficulty")
            return row.totalDifficulty
              ? parseBig(sql, row.totalDifficulty)
              : null;
          if (prop === "transactionsRoot") return row.transactionsRoot;

          return Reflect.get(target, prop, receiver);
        },
      };

      const log: Partial<Log> = {
        id: row.logId,
        address: checksumAddress(row.address),
        data: row.data,
        topics: [row.topic0, row.topic1, row.topic2, row.topic3].filter(
          (t): t is Hex => t !== null,
        ) as [Hex, ...Hex[]] | [],
      };

      const logHandler: ProxyHandler<Log> = {
        get: (target, prop, receiver) => {
          if (prop === "logIndex") return row.logIndex;
          if (prop === "blockHash") return row.blockHash;
          if (prop === "blockNumber") return row.blockNumber;
          if (prop === "transactionHash") return row.transactionHash;
          if (prop === "transactionIndex") return row.transactionIndex;
          if (prop === "removed") return false;
          return Reflect.get(target, prop, receiver);
        },
      };

      const transaction: Partial<Transaction> = {
        hash: row.tx_hash,
      };

      const transactionHandler: ProxyHandler<Transaction> = {
        get: (target, prop, receiver) => {
          if (prop === "blockHash") return row.blockHash;
          if (prop === "blockNumber") return row.blockNumber;
          if (prop === "from") return checksumAddress(row.from);
          if (prop === "gas") return parseBig(sql, row.gas);
          if (prop === "input") return row.input;
          if (prop === "nonce") return Number(row.tx_nonce);
          if (prop === "r") return row.r;
          if (prop === "s") return row.s;
          if (prop === "to") return row.to ? checksumAddress(row.to) : row.to;
          if (prop === "transactionIndex") return Number(row.transactionIndex);
          if (prop === "value") return parseBig(sql, row.value);

          // ...(row.tx_type === "0x0"
          //   ? {
          //       type: "legacy",
          //       gasPrice: parseBig(sql, row.tx_gasPrice),
          //     }
          //   : row.tx_type === "0x1"
          //     ? {
          //         type: "eip2930",
          //         gasPrice: parseBig(sql, row.tx_gasPrice),
          //         accessList: JSON.parse(row.tx_accessList),
          //       }
          //     : row.tx_type === "0x2"
          //       ? {
          //           type: "eip1559",
          //           maxFeePerGas: parseBig(sql, row.tx_maxFeePerGas),
          //           maxPriorityFeePerGas: parseBig(
          //             sql,
          //             row.tx_maxPriorityFeePerGas,
          //           ),
          //         }
          //       : row.tx_type === "0x7e"
          //         ? {
          //             type: "deposit",
          //             maxFeePerGas: row.tx_maxFeePerGas
          //               ? parseBig(sql, row.tx_maxFeePerGas)
          //               : undefined,
          //             maxPriorityFeePerGas: row.tx_maxPriorityFeePerGas
          //               ? parseBig(sql, row.tx_maxPriorityFeePerGas)
          //               : undefined,
          //           }
          //         : {
          //             type: row.tx_type,
          //           })

          return Reflect.get(target, prop, receiver);
        },
      };

      return {
        chainId: filter.chainId,
        sourceIndex: row.event_filterIndex,
        checkpoint: row.event_checkpoint,
        block: new Proxy(block, blockHandler),
        log: hasLog ? new Proxy(log, logHandler) : undefined,
        transaction: hasTransaction
          ? new Proxy(transaction, transactionHandler)
          : undefined,
        trace: hasCallTrace
          ? {
              id: row.callTraceId,
              // from: checksumAddress(row.callTrace_from),
              // to: checksumAddress(row.callTrace_to),
              // gas: parseBig(sql, row.callTrace_gas),
              // value: parseBig(sql, row.callTrace_value),
              // input: row.callTrace_input,
              // output: row.callTrace_output,
              // gasUsed: parseBig(sql, row.callTrace_gasUsed),
              // subtraces: row.callTrace_subtraces,
              // traceAddress: JSON.parse(row.callTrace_traceAddress),
              // blockHash: row.callTrace_blockHash,
              // blockNumber: parseBig(sql, row.callTrace_blockNumber),
              // transactionHash: row.callTrace_transactionHash,
              // transactionIndex: row.callTrace_transactionPosition,
              // callType: row.callTrace_callType as CallTrace["callType"],
            }
          : undefined,
        transactionReceipt: hasTransactionReceipt
          ? {
              // blockHash: row.txr_blockHash,
              // blockNumber: parseBig(sql, row.txr_blockNumber),
              // contractAddress: row.txr_contractAddress
              //   ? checksumAddress(row.txr_contractAddress)
              //   : null,
              // cumulativeGasUsed: parseBig(sql, row.txr_cumulativeGasUsed),
              // effectiveGasPrice: parseBig(sql, row.txr_effectiveGasPrice),
              // from: checksumAddress(row.txr_from),
              // gasUsed: parseBig(sql, row.txr_gasUsed),
              // logs: JSON.parse(row.txr_logs).map((log: SyncLog) => ({
              //   address: checksumAddress(log.address),
              //   blockHash: log.blockHash,
              //   blockNumber: hexToBigInt(log.blockNumber),
              //   data: log.data,
              //   logIndex: hexToNumber(log.logIndex),
              //   removed: false,
              //   topics: [
              //     log.topics[0] ?? null,
              //     log.topics[1] ?? null,
              //     log.topics[2] ?? null,
              //     log.topics[3] ?? null,
              //   ].filter((t): t is Hex => t !== null) as [Hex, ...Hex[]] | [],
              //   transactionHash: log.transactionHash,
              //   transactionIndex: hexToNumber(log.transactionIndex),
              // })),
              // logsBloom: row.txr_logsBloom,
              // status:
              //   row.txr_status === "0x1"
              //     ? "success"
              //     : row.txr_status === "0x0"
              //       ? "reverted"
              //       : (row.txr_status as TransactionReceipt["status"]),
              // to: row.txr_to ? checksumAddress(row.txr_to) : null,
              // transactionHash: row.txr_transactionHash,
              // transactionIndex: Number(row.txr_transactionIndex),
              // type:
              //   row.txr_type === "0x0"
              //     ? "legacy"
              //     : row.txr_type === "0x1"
              //       ? "eip2930"
              //       : row.tx_type === "0x2"
              //         ? "eip1559"
              //         : row.tx_type === "0x7e"
              //           ? "deposit"
              //           : row.tx_type,
            }
          : undefined,
      } as RawEvent;
    });

    common.metrics.ponder_database_decoding_duration.observe(
      { method: "getEvents" },
      endClock(),
    );

    let cursor: string;
    if (events.length !== limit) {
      cursor = to;
    } else {
      cursor = events[events.length - 1]!.checkpoint!;
    }

    return { events, cursor };
  },
  insertRpcRequestResult: async ({ request, blockNumber, chainId, result }) =>
    db.wrap({ method: "insertRpcRequestResult" }, async () => {
      await db
        .insertInto("rpcRequestResults")
        .values({
          request,
          blockNumber: formatBig(sql, blockNumber),
          chainId,
          result,
        })
        .onConflict((oc) =>
          oc
            .columns(["request", "chainId", "blockNumber"])
            .doUpdateSet({ result }),
        )
        .execute();
    }),
  getRpcRequestResult: async ({ request, blockNumber, chainId }) =>
    db.wrap({ method: "getRpcRequestResult" }, async () => {
      const result = await db
        .selectFrom("rpcRequestResults")
        .select("result")
        .where("request", "=", request)
        .where("chainId", "=", chainId)
        .where("blockNumber", "=", formatBig(sql, blockNumber))
        .executeTakeFirst();

      return result?.result ?? null;
    }),
  pruneByBlock: async ({ fromBlock, chainId }) =>
    db.wrap({ method: "pruneByBlock" }, async () => {
      await db.transaction().execute(async (tx) => {
        await tx
          .deleteFrom("logs")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("blocks")
          .where("chainId", "=", chainId)
          .where("number", ">", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("rpcRequestResults")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("callTraces")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">", formatBig(sql, fromBlock))
          .execute();
      });
    }),
  pruneByChain: async ({ fromBlock, chainId }) =>
    db.wrap({ method: "pruneByChain" }, () =>
      db.transaction().execute(async (tx) => {
        await tx
          .with("deleteLogFilter(logFilterId)", (qb) =>
            qb
              .selectFrom("logFilterIntervals")
              .innerJoin("logFilters", "logFilterId", "logFilters.id")
              .select("logFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", ">=", formatBig(sql, fromBlock)),
          )
          .deleteFrom("logFilterIntervals")
          .where(
            "logFilterId",
            "in",
            ksql`(SELECT "logFilterId" FROM ${ksql.table("deleteLogFilter")})`,
          )
          .execute();

        await tx
          .with("updateLogFilter(logFilterId)", (qb) =>
            qb
              .selectFrom("logFilterIntervals")
              .innerJoin("logFilters", "logFilterId", "logFilters.id")
              .select("logFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", "<", formatBig(sql, fromBlock))
              .where("endBlock", ">", formatBig(sql, fromBlock)),
          )
          .updateTable("logFilterIntervals")
          .set({
            endBlock: formatBig(sql, fromBlock),
          })
          .where(
            "logFilterId",
            "in",
            ksql`(SELECT "logFilterId" FROM ${ksql.table("updateLogFilter")})`,
          )
          .execute();

        await tx
          .with("deleteFactoryLogFilter(factoryId)", (qb) =>
            qb
              .selectFrom("factoryLogFilterIntervals")
              .innerJoin(
                "factoryLogFilters",
                "factoryId",
                "factoryLogFilters.id",
              )

              .select("factoryId")
              .where("chainId", "=", chainId)
              .where("startBlock", ">=", formatBig(sql, fromBlock)),
          )
          .deleteFrom("factoryLogFilterIntervals")
          .where(
            "factoryId",
            "in",
            ksql`(SELECT "factoryId" FROM ${ksql.table("deleteFactoryLogFilter")})`,
          )
          .execute();

        await tx
          .with("updateFactoryLogFilter(factoryId)", (qb) =>
            qb
              .selectFrom("factoryLogFilterIntervals")
              .innerJoin(
                "factoryLogFilters",
                "factoryId",
                "factoryLogFilters.id",
              )

              .select("factoryId")
              .where("chainId", "=", chainId)
              .where("startBlock", "<", formatBig(sql, fromBlock))
              .where("endBlock", ">", formatBig(sql, fromBlock)),
          )
          .updateTable("factoryLogFilterIntervals")
          .set({
            endBlock: formatBig(sql, fromBlock),
          })
          .where(
            "factoryId",
            "in",
            ksql`(SELECT "factoryId" FROM ${ksql.table("updateFactoryLogFilter")})`,
          )
          .execute();

        await tx
          .with("deleteTraceFilter(traceFilterId)", (qb) =>
            qb
              .selectFrom("traceFilterIntervals")
              .innerJoin("traceFilters", "traceFilterId", "traceFilters.id")
              .select("traceFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", ">=", formatBig(sql, fromBlock)),
          )
          .deleteFrom("traceFilterIntervals")
          .where(
            "traceFilterId",
            "in",
            ksql`(SELECT "traceFilterId" FROM ${ksql.table("deleteTraceFilter")})`,
          )
          .execute();

        await tx
          .with("updateTraceFilter(traceFilterId)", (qb) =>
            qb
              .selectFrom("traceFilterIntervals")
              .innerJoin("traceFilters", "traceFilterId", "traceFilters.id")
              .select("traceFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", "<", formatBig(sql, fromBlock))
              .where("endBlock", ">", formatBig(sql, fromBlock)),
          )
          .updateTable("traceFilterIntervals")
          .set({
            endBlock: formatBig(sql, fromBlock),
          })
          .where(
            "traceFilterId",
            "in",
            ksql`(SELECT "traceFilterId" FROM ${ksql.table("updateTraceFilter")})`,
          )
          .execute();

        await tx
          .with("deleteFactoryTraceFilter(factoryId)", (qb) =>
            qb
              .selectFrom("factoryTraceFilterIntervals")
              .innerJoin(
                "factoryTraceFilters",
                "factoryId",
                "factoryTraceFilters.id",
              )
              .select("factoryId")
              .where("chainId", "=", chainId)
              .where("startBlock", ">=", formatBig(sql, fromBlock)),
          )
          .deleteFrom("factoryTraceFilterIntervals")
          .where(
            "factoryId",
            "in",
            ksql`(SELECT "factoryId" FROM ${ksql.table("deleteFactoryTraceFilter")})`,
          )
          .execute();

        await tx
          .with("updateFactoryTraceFilter(factoryId)", (qb) =>
            qb
              .selectFrom("factoryTraceFilterIntervals")
              .innerJoin(
                "factoryTraceFilters",
                "factoryId",
                "factoryTraceFilters.id",
              )

              .select("factoryId")
              .where("chainId", "=", chainId)
              .where("startBlock", "<", formatBig(sql, fromBlock))
              .where("endBlock", ">", formatBig(sql, fromBlock)),
          )
          .updateTable("factoryTraceFilterIntervals")
          .set({
            endBlock: formatBig(sql, fromBlock),
          })
          .where(
            "factoryId",
            "in",
            ksql`(SELECT "factoryId" FROM ${ksql.table("updateFactoryTraceFilter")})`,
          )
          .execute();

        await tx
          .with("deleteBlockFilter(blockFilterId)", (qb) =>
            qb
              .selectFrom("blockFilterIntervals")
              .innerJoin("blockFilters", "blockFilterId", "blockFilters.id")
              .select("blockFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", ">=", formatBig(sql, fromBlock)),
          )
          .deleteFrom("blockFilterIntervals")
          .where(
            "blockFilterId",
            "in",
            ksql`(SELECT "blockFilterId" FROM ${ksql.table("deleteBlockFilter")})`,
          )
          .execute();

        await tx
          .with("updateBlockFilter(blockFilterId)", (qb) =>
            qb
              .selectFrom("blockFilterIntervals")
              .innerJoin("blockFilters", "blockFilterId", "blockFilters.id")
              .select("blockFilterId")
              .where("chainId", "=", chainId)
              .where("startBlock", "<", formatBig(sql, fromBlock))
              .where("endBlock", ">", formatBig(sql, fromBlock)),
          )
          .updateTable("blockFilterIntervals")
          .set({
            endBlock: formatBig(sql, fromBlock),
          })
          .where(
            "blockFilterId",
            "in",
            ksql`(SELECT "blockFilterId" FROM ${ksql.table("updateBlockFilter")})`,
          )
          .execute();

        await tx
          .deleteFrom("logs")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">=", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("blocks")
          .where("chainId", "=", chainId)
          .where("number", ">=", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("rpcRequestResults")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">=", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("callTraces")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">=", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("transactions")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">=", formatBig(sql, fromBlock))
          .execute();
        await tx
          .deleteFrom("transactionReceipts")
          .where("chainId", "=", chainId)
          .where("blockNumber", ">=", formatBig(sql, fromBlock))
          .execute();
      }),
    ),
});
