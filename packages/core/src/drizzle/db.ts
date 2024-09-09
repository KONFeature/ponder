import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type { PgliteDatabase } from "drizzle-orm/pglite";

export type DrizzleDb = Pick<
  PgliteDatabase | NodePgDatabase,
  "select" | "execute"
>;
