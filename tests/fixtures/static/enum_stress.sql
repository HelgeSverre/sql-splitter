-- Enum stress fixture: exercises bidirectional PG↔MySQL conversion edge cases.
-- Schema-qualified and unqualified types, ALTER TYPE ADD VALUE with
-- BEFORE/AFTER, IF [NOT] EXISTS, quoted identifiers, special characters in
-- labels, empty enum, constraint naming collision, dedupe reuse.

-- ============================================================
-- PostgreSQL source
-- ============================================================

CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');
CREATE TYPE IF NOT EXISTS public.order_status AS ENUM ('pending', 'shipped', 'cancelled', 'delivered');
CREATE TYPE empty_enum AS ENUM ();
CREATE TYPE "Order Type" AS ENUM ('online', 'in-store');
CREATE TYPE status AS ENUM ('active', 'inactive');

ALTER TYPE public.mood ADD VALUE 'ecstatic' AFTER 'happy';
ALTER TYPE mood ADD VALUE IF NOT EXISTS 'anxious';

CREATE TABLE person (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    m     mood NOT NULL,
    m2    public.mood NOT NULL
);

CREATE TABLE orders (
    id      SERIAL PRIMARY KEY,
    status  public.order_status NOT NULL,
    type    "Order Type",
    note    TEXT DEFAULT 'status: pending'
);

-- Constraint named after an enum: must not be replaced.
CREATE TABLE works (
    id        SERIAL PRIMARY KEY,
    s         status,
    e         empty_enum,
    CONSTRAINT mood CHECK (s IS NOT NULL)
);

INSERT INTO person (id, name, m, m2) VALUES (1, 'Alice', 'happy', 'ok');
INSERT INTO orders (id, status, type, note) VALUES (100, 'pending', 'online', 'status: pending');

-- ============================================================
-- MySQL source
-- ============================================================

CREATE TABLE `film` (
  `id`     INT PRIMARY KEY,
  `rating` ENUM('G','PG','PG-13','R','NC-17') NOT NULL,
  `status` ENUM('active','inactive') DEFAULT 'active',
  `labels` ENUM('a)','b','it''s','back\\slash','✅')
) ENGINE=InnoDB;
