import type { PonderOptions } from "@ponder/ponder";
import type { GraphQLSchema } from "graphql";
import { printSchema } from "graphql";
import { writeFileSync } from "node:fs";
import path from "node:path";

const header = `
""" Autogenerated file. Do not edit manually. """
`;

export const generateSchema = (
  gqlSchema: GraphQLSchema,
  options: PonderOptions
) => {
  const body = printSchema(gqlSchema);

  const final = header + body;

  writeFileSync(
    path.join(options.GENERATED_DIR_PATH, "schema.graphql"),
    final,
    "utf8"
  );
};