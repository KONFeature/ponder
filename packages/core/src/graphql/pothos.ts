import type { DrizzleDb } from "@/drizzle/db.js";
import type { MetadataStore } from "@/indexing-store/store.js";
import type { ReferenceColumn, Schema } from "@/schema/common.js";
import {
  extractReferenceTable,
  getTables,
  isEnumColumn,
  isJSONColumn,
  isListColumn,
  isManyColumn,
  isOneColumn,
  isOptionalColumn,
  isReferenceColumn,
  isScalarColumn,
} from "@/schema/utils.js";
import { getEnums } from "@/schema/utils.js";
import SchemaBuilder, {
  type BaseScalarNames,
  type FieldMap,
  type InputFieldMap,
} from "@pothos/core";
import DataloaderPlugin from "@pothos/plugin-dataloader";
import { inArray } from "drizzle-orm";
import { printSchema } from "graphql";
import { GraphQLJSON } from "./graphQLJson.js";
import { GraphQLBigInt, SCALARS2 } from "./scalar.js";

const filterOperators = {
  universal: ["", "_not"],
  singular: ["_in", "_not_in"],
  plural: ["_has", "_not_has"],
  numeric: ["_gt", "_lt", "_gte", "_lte"],
  string: [
    "_contains",
    "_not_contains",
    "_starts_with",
    "_ends_with",
    "_not_starts_with",
    "_not_ends_with",
  ],
} as const;

export function createGraphqlSchema(schema: Schema) {
  const builder = new SchemaBuilder<{
    Context: {
      db: DrizzleDb;
      tables: { [tableName: string]: any };
      metadataStore: MetadataStore;
    };
    Scalars: {
      BigInt: { Input: bigint; Output: bigint };
      JSON: { Input: any; Output: any };
    };
    Objects: {
      PageInfo: {
        hasNextPage: boolean;
        hasPreviousPage: boolean;
        startCursor: string | null;
        endCursor: string | null;
      };
    };
  }>({
    plugins: [DataloaderPlugin],
  });

  builder.addScalarType("BigInt", GraphQLBigInt);
  builder.addScalarType("JSON", GraphQLJSON);

  const enums = getEnums(schema);
  const tables = getTables(schema);

  // Create enum types.
  for (const [enumName, enumValues] of Object.entries(enums)) {
    builder.enumType(enumName, { values: enumValues });
  }

  // Create table filter input types.
  for (const [tableName, { table }] of Object.entries(tables)) {
    const filterTypeName = `${tableName}Filter`;
    const filterType = builder.inputType(filterTypeName, {
      fields: (t) => {
        const fields: InputFieldMap = {
          AND: t.field({ type: [filterType] }),
          OR: t.field({ type: [filterType] }),
        };

        for (const [columnName, column] of Object.entries(table)) {
          if (isOneColumn(column)) continue;
          if (isManyColumn(column)) continue;
          if (isJSONColumn(column)) continue;

          const columnType = (
            isEnumColumn(column) ? column[" enum"] : SCALARS2[column[" scalar"]]
          ) as BaseScalarNames;

          if (isListColumn(column)) {
            // List fields => universal, plural
            for (const suffix of filterOperators.universal) {
              fields[`${columnName}${suffix}`] = t.field({
                type: [columnType],
              });
            }

            for (const suffix of filterOperators.plural) {
              fields[`${columnName}${suffix}`] = t.field({
                type: columnType,
              });
            }
          } else {
            // Scalar fields => universal, singular, numeric OR string depending on base type
            // Note: Booleans => universal and singular only.
            for (const suffix of filterOperators.universal) {
              fields[`${columnName}${suffix}`] = t.field({
                type: columnType,
              });
            }

            for (const suffix of filterOperators.singular) {
              fields[`${columnName}${suffix}`] = t.field({
                type: [columnType],
              });
            }

            if (
              (isScalarColumn(column) || isReferenceColumn(column)) &&
              ["int", "bigint", "float", "hex"].includes(column[" scalar"])
            ) {
              for (const suffix of filterOperators.numeric) {
                fields[`${columnName}${suffix}`] = t.field({
                  type: columnType,
                });
              }
            }

            if (
              (isScalarColumn(column) || isReferenceColumn(column)) &&
              "string" === column[" scalar"]
            ) {
              for (const suffix of filterOperators.string) {
                fields[`${columnName}${suffix}`] = t.field({
                  type: columnType,
                });
              }
            }
          }
        }

        return fields;
      },
    });
  }

  builder.objectType("PageInfo", {
    fields: (t) => ({
      hasNextPage: t.exposeBoolean("hasNextPage"),
      hasPreviousPage: t.exposeBoolean("hasPreviousPage"),
      startCursor: t.exposeString("startCursor"),
      endCursor: t.exposeString("endCursor"),
    }),
  });

  // Create table page and object types.
  for (const [tableName, { table }] of Object.entries(tables)) {
    const pageType = builder.objectType(`${tableName}Page` as any, {
      fields: (t) => ({
        items: t.expose("items", { type: [objectType] }),
        pageInfo: t.expose("pageInfo", { type: "PageInfo" }),
      }),
    });

    const objectType = builder.loadableObject(tableName, {
      load: async (ids: string[], context) => {
        const table = context.tables[tableName];
        return await context.db
          .select()
          .from(table)
          .where(inArray(table.id, ids));
      },
      sort: (object: any) => object.id,
      fields: (t) => {
        const fields: FieldMap = {};

        for (const [columnName, column] of Object.entries(table)) {
          if (isOneColumn(column)) {
            // Column must resolve the foreign key of the referenced column
            // Note: this relies on the fact that reference columns can't be lists.
            const referenceColumn = table[
              column[" reference"]
            ] as ReferenceColumn;
            const referencedTable = extractReferenceTable(referenceColumn);

            fields[columnName] = t.field({
              type: referencedTable as "String",
              nullable: isOptionalColumn(referenceColumn),
              // Use dataloader to resolve the referenced record by ID.
              resolve: async (parent: any) => parent[column[" reference"]],
            });
          } else if (isManyColumn(column)) {
            fields[columnName] = t.field({
              type: pageType,
              args: {
                where: t.arg({ type: `${tableName}Filter` as "String" }),
                orderBy: t.arg({ type: "String" }),
                orderDirection: t.arg({ type: "String" }),
                before: t.arg({ type: "String" }),
                after: t.arg({ type: "String" }),
                limit: t.arg({ type: "Int" }),
              },
              resolve: async (parent: any, args, context) => {
                const { where, orderBy, orderDirection, limit, after, before } =
                  args;

                const whereObject = where ? buildWhereObject(where) : {};
                // Add the parent record ID to the where object.
                // Note that this overrides any existing equals condition.
                (whereObject[column[" referenceColumn"]] ??= {}).equals =
                  parent.id;

                const orderByObject = orderBy
                  ? { [orderBy]: orderDirection ?? "asc" }
                  : undefined;

                // Query for the IDs of the matching records.
                // TODO: Update query to only fetch IDs, not entire records.
                const result = await context.readonlyStore.findMany({
                  tableName: column[" referenceTable"],
                  where: whereObject,
                  orderBy: orderByObject,
                  limit,
                  before,
                  after,
                });

                return { items, pageInfo: result.pageInfo };
              },
            });
          } else {
            const columnType = (
              isJSONColumn(column)
                ? "JSON"
                : isEnumColumn(column)
                  ? column[" enum"]
                  : SCALARS2[column[" scalar"]]
            ) as BaseScalarNames;

            fields[columnName] = t.expose(columnName, {
              type: isListColumn(column) ? [columnType] : columnType,
              nullable: isOptionalColumn(column),
            });
          }
        }

        return fields;
      },
    });
  }

  const graphqlSchema = builder.toSchema();

  console.log(printSchema(graphqlSchema));

  return graphqlSchema;

  // const { entityTypes, entityPageTypes } = buildEntityTypes({
  //   schema,
  //   enumTypes,
  //   entityFilterTypes,
  // });

  // builder.objectType("Entity",
  // {
  //   fields: (t) => ({
  //     kek: t.boolean({}),
  //   }),
  // }
  // )

  // builder.queryType({
  //   fields: (t) => ({
  //     hello: t.string({
  //       args: {
  //         name: t.arg.string(),
  //       },
  //       resolve: (parent, { name }) => `hello, ${name || "World"}`,
  //     }),
  //   }),
  // });
}
