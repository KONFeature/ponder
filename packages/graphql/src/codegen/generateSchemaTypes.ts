import { codegen } from "@graphql-codegen/core";
import * as typescriptPlugin from "@graphql-codegen/typescript";
import type { PonderPluginArgument } from "@ponder/ponder";
import { GraphQLSchema, parse, printSchema } from "graphql";
import { writeFileSync } from "node:fs";
import path from "node:path";

const header = `
/* Autogenerated file. Do not edit manually. */
`;

export const generateSchemaTypes = async (
  gqlSchema: GraphQLSchema,
  ponder: PonderPluginArgument
) => {
  const body = await codegen({
    documents: [],
    config: {},
    // used by a plugin internally, although the 'typescript' plugin currently
    // returns the string output, rather than writing to a file
    filename: "",
    schema: parse(printSchema(gqlSchema)),
    plugins: [
      {
        typescript: {},
      },
    ],
    pluginMap: {
      typescript: typescriptPlugin,
    },
  });

  const final = ponder.prettier(header + body);

  writeFileSync(
    path.join(ponder.options.GENERATED_DIR_PATH, "schema.ts"),
    final,
    "utf8"
  );
};
