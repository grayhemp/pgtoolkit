-- Test cluster script

\c postgres
DROP DATABASE IF EXISTS dbname1;
DROP DATABASE IF EXISTS dbname2;
--
CREATE DATABASE dbname1;
CREATE DATABASE dbname2;
--
\c dbname1
--
CREATE EXTENSION pgstattuple;
--
CREATE TABLE table1 AS
SELECT
    i AS id,
    repeat('blabla'||i::text, (random() * 500)::integer) AS text_column,
    now() - '1 year'::interval * random() AS timestamp_column,
    random() < 0.5 AS boolean_column,
    random() + i * 2 AS float_column,
    null::text AS null_column,
    CASE
        WHEN random() < 0.5
        THEN random()
        ELSE NULL END AS partially_null_column
FROM generate_series(1, 10000) i;
ALTER TABLE table1 ADD CONSTRAINT table1_pkey PRIMARY KEY (id);
ALTER TABLE table1 ADD CONSTRAINT table1_uidx1 UNIQUE (id, text_column)
WITH (fillfactor=50);
CREATE INDEX table1_idx1 ON table1 (text_column, float_column);
--CREATE INDEX table1_gist ON table1
--USING gist (to_tsvector('english', id::text));
--CREATE INDEX table1_gin ON table1
--USING gin (to_tsvector('english', id::text));
CREATE INDEX table1_hash ON table1 USING hash (text_column);
DELETE FROM table1 WHERE random() < 0.5;
CREATE INDEX table1_idx2 ON table1 (text_column, float_column);
CREATE INDEX table1_idx3 ON table1 (text_column) WHERE false;
CREATE INDEX table1_idx4 ON table1 (text_column) WHERE  id < 500;
--
CREATE TABLE table2 ("primary" integer, float_column real)
WITH (fillfactor=50);
INSERT INTO table2
SELECT
    i AS "primary",
    random() * 10000 AS float_column
FROM generate_series(1, 80000) i;
DELETE FROM table2 WHERE random() < 0.5;
--
CREATE TABLE table3 (id integer, text_column text);
ALTER TABLE table3 ADD CONSTRAINT table3_pkey PRIMARY KEY (id);
ALTER TABLE table3 ADD CONSTRAINT table3_fkey
    FOREIGN KEY (id, text_column) REFERENCES table1 (id, text_column)
    ON UPDATE RESTRICT ON DELETE CASCADE;
INSERT INTO table3 SELECT id FROM table1;
--
CREATE TABLE "таблица2" (id bigserial PRIMARY KEY, text_column text);
--
CREATE SCHEMA dummy;
--
ALTER DATABASE dbname1 SET search_path TO dummy;
--
\c dbname2
--
--CREATE EXTENSION pgstattuple;
--
CREATE TABLE table1 ("primary" integer, float_column real)
WITH (fillfactor=50);
INSERT INTO table1
SELECT
    i AS "primary",
    random() * 10000 AS float_column
FROM generate_series(1, 8000) i;
DELETE FROM table1 WHERE random() < 0.5;
--
CREATE TABLE table2 ("primary" integer, float_column real)
WITH (autovacuum_vacuum_scale_factor=0.02);
INSERT INTO table2
SELECT
    i AS "primary",
    random() * 10000 AS float_column
FROM generate_series(1, 5000) i;
DELETE FROM table2 WHERE random() < 0.5;
--
CREATE TABLE table3 (
    "primary" integer, float_column real, partially_null_column real)
WITHOUT OIDS;
INSERT INTO table3
SELECT
    i AS id,
    random() * 10000 AS float_column,
    CASE
        WHEN random() < 0.5
        THEN random()
        ELSE NULL END AS partially_null_column
FROM generate_series(1, 5000) i;
DELETE FROM table3 WHERE random() < 0.05;
--
CREATE TABLE table4 (
    "primary" integer, float_column real, partially_null_column real)
WITHOUT OIDS;
INSERT INTO table4
SELECT
    i AS id,
    random() * 10000 AS float_column,
    CASE
        WHEN random() < 0.5
        THEN random()
        ELSE NULL END AS partially_null_column
FROM generate_series(1, 5000) i;
DELETE FROM table4 WHERE random() < 0.5;
--
CREATE SCHEMA schema1;
--
CREATE TABLE schema1.table1 AS
SELECT
    i AS id,
    random() * 10000 AS float_column
FROM generate_series(1, 10) i;
--
CREATE TABLE public.table5 AS
SELECT
    i AS id,
    random() * 10000 AS float_column,
    repeat('blabla'||i::text, (random() * 500)::integer) AS text_column
FROM generate_series(1, 100000) i;
UPDATE public.table5 SET float_column = random() * 10000;
--
CREATE TABLE public.table7 AS
SELECT
    i AS id,
    random() * 10000 AS float_column,
    repeat('blabla'||i::text, (random() * 500)::integer) AS text_column
FROM generate_series(1, 100000) AS i;
DELETE FROM public.table7 WHERE id BETWEEN 10 AND 100000 - 10;
--
CREATE SCHEMA dummy;
--
ALTER DATABASE dbname2 SET search_path TO dummy;
--
\c dbname1

-- Rewrite the clean table function

-- Calculate the maximum possible number of tuples per page for
-- a table (i_max_tupples_per_page)
SELECT ceil(current_setting('block_size')::real / sum(attlen))
FROM pg_attribute
WHERE
    attrelid = 'table1'::regclass AND
    attnum < 0;

CREATE OR REPLACE FUNCTION public._clean_pages(
    i_table_ident text,
    i_column_ident text,
    i_to_page integer,
    i_page_offset integer,
    i_max_tupples_per_page integer)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    _from_page integer := i_to_page - i_page_offset + 1;
    _min_ctid tid;
    _max_ctid tid;
    _ctid_list tid[];
    _next_ctid_list tid[];
    _ctid tid;
    _loop integer;
    _result_page integer;
    _update_query text :=
        'UPDATE ONLY ' || i_table_ident ||
        ' SET ' || i_column_ident || ' = ' || i_column_ident ||
        ' WHERE ctid = ANY($1) RETURNING ctid';
BEGIN
    -- Check page argument values
    IF NOT (
        i_page_offset IS NOT NULL AND i_page_offset >= 1 AND
        i_to_page IS NOT NULL AND i_to_page >= 1 AND
        i_to_page >= i_page_offset)
    THEN
        RAISE EXCEPTION 'Wrong page arguments specified.';
    END IF;

    -- Check that session_replication_role is set to replica to
    -- prevent triggers firing
    IF NOT (
        SELECT setting = 'replica'
        FROM pg_catalog.pg_settings
        WHERE name = 'session_replication_role')
    THEN
        RAISE EXCEPTION 'The session_replication_role must be set to replica.';
    END IF;

    -- Define minimal and maximal ctid values of the range
    _min_ctid := (_from_page, 1)::text::tid;
    _max_ctid := (i_to_page, i_max_tupples_per_page)::text::tid;

    -- Build a list of possible ctid values of the range
    SELECT array_agg((pi, ti)::text::tid)
    INTO _ctid_list
    FROM generate_series(_from_page, i_to_page) AS pi
    CROSS JOIN generate_series(1, i_max_tupples_per_page) AS ti;

    <<_outer_loop>>
    FOR _loop IN 1..i_max_tupples_per_page LOOP
        _next_ctid_list := array[]::tid[];

        -- Update all the tuples in the range
        FOR _ctid IN EXECUTE _update_query USING _ctid_list
        LOOP
            IF _ctid > _max_ctid THEN
                RAISE EXCEPTION 'No more free space left in the table.';
            ELSIF _ctid >= _min_ctid THEN
                -- The tuple is still in the range, more updates are needed
                _next_ctid_list := _next_ctid_list || _ctid;
            END IF;
        END LOOP;

        _ctid_list := _next_ctid_list;

        -- Finish processing if there are no tupples in the range left
        IF coalesce(array_length(_ctid_list, 1), 0) = 0 THEN
            _result_page := _from_page - 1;
            EXIT _outer_loop;
        END IF;
    END LOOP;

    -- No result
    IF _loop = i_max_tupples_per_page AND _result_page IS NULL THEN
        RAISE EXCEPTION
            'Maximal loops count has been reached with no result.';
    END IF;

    RETURN _result_page;
END $$;

-- Get statistics queries

SELECT
    size,
    total_size,
    ceil(size::real / bs) AS page_count,
    ceil(total_size::real / bs) AS total_page_count
FROM (
    SELECT
        current_setting('block_size')::integer AS bs,
        pg_catalog.pg_relation_size('public.table2') AS size,
        pg_catalog.pg_total_relation_size('public.table2') AS total_size
) AS sq;

SELECT
    ceil(pure_page_count * 100 / fillfactor) AS effective_page_count,
    CASE WHEN size::real > 0 THEN
        round(
            100 * (
                1 - (pure_page_count * 100 / fillfactor) / (size::real / bs)
            )::numeric, 2
        )
    ELSE 0 END AS free_percent,
    ceil(size::real - bs * pure_page_count * 100 / fillfactor) AS free_space
FROM (
    SELECT
        bs, size, fillfactor,
        ceil(
            reltuples * (
                max(stanullfrac) * ma * ceil(
                    (
                        ma * ceil(
                            (
                                header_width +
                                ma * ceil(count(1)::real / ma)
                            )::real / ma
                        ) + sum((1 - stanullfrac) * stawidth)
                    )::real / ma
                ) +
                (1 - max(stanullfrac)) * ma * ceil(
                    (
                        ma * ceil(header_width::real / ma) +
                        sum((1 - stanullfrac) * stawidth)
                    )::real / ma
                )
            )::real / (bs - 24)
        ) AS pure_page_count
    FROM (
        SELECT
            pg_catalog.pg_class.oid AS class_oid,
            reltuples,
            23 AS header_width, 8 AS ma,
            current_setting('block_size')::integer AS bs,
            pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
            coalesce(
                (
                    SELECT (
                        regexp_matches(
                            reloptions::text, E'.*fillfactor=(\\d+).*'))[1]),
                '100')::real AS fillfactor
        FROM pg_catalog.pg_class
        WHERE pg_catalog.pg_class.oid = 'public.table5'::regclass
    ) AS const
    LEFT JOIN pg_catalog.pg_statistic ON starelid = class_oid
    GROUP BY bs, class_oid, fillfactor, ma, size, reltuples, header_width
) AS sq;

EXPLAIN (ANALYZE, VERBOSE)
SELECT
    ceil((size - free_space) * 100 / fillfactor / bs) AS effective_page_count,
    round(
        (100 * (1 - (100 - free_percent) / fillfactor))::numeric, 2
    ) AS free_percent,
    ceil(size - (size - free_space) * 100 / fillfactor) AS free_space
FROM (
    SELECT
        current_setting('block_size')::integer AS bs,
        pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
        coalesce(
            (
                SELECT (
                    regexp_matches(
                        reloptions::text, E'.*fillfactor=(\\d+).*'))[1]),
            '100')::real AS fillfactor,
        pgst.*
    FROM pg_catalog.pg_class
    CROSS JOIN public.pgstattuple('public.table2') AS pgst
    WHERE pg_catalog.pg_class.oid = 'public.table2'::regclass
) AS sq;

CREATE TABLE public.table1 AS
SELECT repeat('blabla'||i::text, (random() * 500)::integer) AS text_column
FROM generate_series(1, 1000000) i;
DELETE FROM public.table1 WHERE random() < 0.5;

SELECT public.pg_relpages('public.table1');
SELECT
    pg_catalog.pg_relation_size('public.table1')::real /
    current_setting('block_size')::integer;

DROP TABLE public.table1;

-- Check special triggers

SELECT count(1) FROM pg_catalog.pg_trigger
WHERE
    tgrelid = 'public.table1'::regclass AND
    tgtype & 16 = 8 AND
    tgenabled IN ('A', 'R');

-- Get index definitions

SELECT
    indexname, tablespace, indexdef,
    regexp_replace(indexdef, E'.* USING (\\w+) .*', E'\\1') AS indmethod,
    conname,
    CASE
        WHEN contype = 'p' THEN 'PRIMARY KEY'
        WHEN contype = 'u' THEN 'UNIQUE'
        ELSE NULL END AS contypedef,
    (
        SELECT
            bool_and(
                deptype IN ('n', 'a', 'i') AND
                NOT (refobjid = indexoid AND deptype = 'n') AND
                NOT (
                    objid = indexoid AND deptype = 'i' AND
                    (version < array[9,1] OR contype NOT IN ('p', 'u'))))
        FROM pg_catalog.pg_depend
        LEFT JOIN pg_catalog.pg_constraint ON
            pg_catalog.pg_constraint.oid = refobjid
        WHERE objid = indexoid OR refobjid = indexoid
    )::integer AS allowed,
    pg_catalog.pg_relation_size(indexoid)
FROM (
    SELECT
        indexname, tablespace, indexdef,
        (
            quote_ident(schemaname) || '.' ||
            quote_ident(indexname))::regclass AS indexoid,
        string_to_array(
            regexp_replace(
                version(), E'.*PostgreSQL (\\d+\\.\\d+).*', E'\\1'),
            '.')::integer[] AS version
    FROM pg_catalog.pg_indexes
    WHERE
        schemaname = 'public' AND
        tablename = 'table1'
) AS sq
LEFT JOIN pg_catalog.pg_constraint ON
    conindid = indexoid AND contype IN ('p', 'u')
ORDER BY 8;

SELECT size, ceil(size / bs) AS page_count
FROM (
    SELECT
        pg_catalog.pg_relation_size('public."table1_uidx"'::regclass) AS size,
        current_setting('block_size')::real AS bs
) AS sq;

--EXPLAIN (ANALYZE, VERBOSE)
SELECT
    CASE
        WHEN avg_leaf_density = 'NaN' THEN 0
        ELSE
            round(
                (100 * (1 - avg_leaf_density / fillfactor))::numeric, 2
            )
        END AS free_percent,
    CASE
        WHEN avg_leaf_density = 'NaN' THEN 0
        ELSE
            ceil(
                index_size * (1 - avg_leaf_density / fillfactor)
            )
        END AS free_space
FROM (
    SELECT
        coalesce(
            (
                SELECT (
                    regexp_matches(
                        reloptions::text, E'.*fillfactor=(\\d+).*'))[1]),
            '90')::real AS fillfactor,
        pgsi.*
    FROM pg_catalog.pg_class
    CROSS JOIN public.pgstatindex('public."table1_pkey"') AS pgsi
    WHERE pg_catalog.pg_class.oid = 'public."table1_pkey"'::regclass
) AS oq;

-- Check schema existence

SELECT count(1) FROM pg_catalog.pg_namespace WHERE nspname = 'public';

-- Get pgstattuple schema

SELECT nspname FROM pg_catalog.pg_proc
JOIN pg_catalog.pg_namespace AS n ON pronamespace = n.oid
WHERE proname = 'pgstattuple' LIMIT 1;

-- Get dbname list

SELECT datname FROM pg_catalog.pg_database
WHERE
    --datname IN ('dbname1') AND
    --datname NOT IN ('dbname1') AND
    datname NOT IN ('postgres', 'template0', 'template1')
ORDER BY pg_catalog.pg_database_size(datname), datname;

-- Get table name list

SELECT schemaname, tablename FROM pg_catalog.pg_tables
WHERE
    --schemaname IN ('public') AND
    --schemaname NOT IN ('public') AND
    --tablename IN ('table1') AND
    --tablename NOT IN ('table1') AND
    schemaname NOT IN ('pg_catalog', 'information_schema') AND
    schemaname !~ 'pg_.*'
ORDER BY
    pg_catalog.pg_relation_size(
        quote_ident(schemaname) || '.' || quote_ident(tablename)),
    schemaname, tablename;

--
