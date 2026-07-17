--
-- PostgreSQL database dump where TWO tables' COPY data both appear before
-- either table's CREATE TABLE. Exercises schema-late COPY routing: without an
-- explicitly tracked open-block table, a predicate scan over pending tables
-- would misroute one table's rows into the other. Row counts and per-column
-- values must land in the CORRECT table.
--

COPY public.alpha (id, label) FROM stdin;
1	alpha-one
2	alpha-two
3	alpha-three
\.

COPY public.beta (id, note) FROM stdin;
10	beta-ten
20	beta-twenty
\.

CREATE TABLE public.alpha (
    id integer NOT NULL,
    label text NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE public.beta (
    id integer NOT NULL,
    note text NOT NULL,
    PRIMARY KEY (id)
);
