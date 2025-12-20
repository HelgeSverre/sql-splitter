-- Test fixture: Minimal SQLite multi-tenant schema
-- Used for quick integration tests

CREATE TABLE "tenants" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL
);

CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "tenant_id" INTEGER NOT NULL REFERENCES "tenants"("id"),
  "email" TEXT NOT NULL
);

INSERT INTO "tenants" ("id", "name") VALUES (1, 'Tenant A'), (2, 'Tenant B');
INSERT INTO "users" ("id", "tenant_id", "email") VALUES
(1, 1, 'user1@a.com'),
(2, 1, 'user2@a.com'),
(3, 2, 'user1@b.com');
