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
ALTER TABLE table1 ADD CONSTRAINT table1_uidx UNIQUE (float_column)
WITH (fillfactor=50);
CREATE INDEX table1_idx1 ON table1 (text_column, float_column);
DELETE FROM table1 WHERE random() < 0.5;
CREATE INDEX table1_idx2 ON table1 (text_column, float_column);
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
CREATE TABLE table2 AS
SELECT
    i AS "primary",
    random() * 10000 AS float_column
FROM generate_series(1, 5000) i;
DELETE FROM table2 WHERE random() < 0.5;
--
CREATE TABLE table3 AS
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
CREATE SCHEMA schema1;
--
CREATE TABLE schema1.table1 AS
SELECT
    i AS id,
    random() * 10000 AS float_column
FROM generate_series(1, 10) i;
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
        i_page_offset IS NOT NULL OR i_page_offset > 1 OR
        i_to_page IS NOT NULL OR i_to_page > 1 OR
        i_to_page > i_page_offset)
    THEN
        RAISE EXCEPTION 'Wrong page arguments specified.';
    END IF;

    -- Prevent triggers firing on update
    SET LOCAL session_replication_role TO replica;

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
        pg_catalog.pg_relation_size('public.t2') AS size,
        pg_catalog.pg_total_relation_size('public.t2') AS total_size
) AS sq;

SELECT
    effective_page_count,
    CASE
        WHEN
            effective_page_count = 0 OR page_count <= 1 OR
            page_count < effective_page_count
        THEN 0
        ELSE
            round(
                100 * (
                    (page_count - effective_page_count)::real /
                    page_count
                )::numeric, 2
            )
        END AS free_percent,
    CASE
        WHEN page_count < effective_page_count THEN 0
        ELSE round(bs * (page_count - effective_page_count))
        END AS free_space
FROM (
    SELECT
        bs,
        ceil(size / bs) AS page_count,
        ceil(
            (fillfactor::real / 100) * size / bs + reltuples *
            (
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
        ) AS effective_page_count
    FROM (
        SELECT
            pg_catalog.pg_class.oid AS class_oid,
            reltuples,
            23 AS header_width, 8 AS ma,
            current_setting('block_size')::integer AS bs,
            pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
            coalesce(
                regexp_replace(
                    reloptions::text, E'.*fillfactor=(\\d+).*', E'\\1'),
                '10')::integer AS fillfactor
        FROM pg_catalog.pg_class
        WHERE pg_catalog.pg_class.oid = 'public.table1'::regclass
    ) AS const
    LEFT JOIN pg_catalog.pg_statistic ON starelid = class_oid
    GROUP BY bs, class_oid, fillfactor, ma, size, reltuples, header_width
) AS sq;

SELECT
    CASE
        WHEN free_percent = 0 THEN page_count
        ELSE ceil(page_count * (1 - free_percent::real / 100))
        END AS effective_page_count,
    CASE WHEN free_percent < 0 THEN 0 ELSE free_percent END AS free_percent,
    CASE WHEN free_space < 0 THEN 0 ELSE free_space END AS free_space
FROM (
    SELECT
        free_percent - fillfactor AS free_percent,
        free_space - ceil(size::real * fillfactor / 100) AS free_space,
        ceil(size::real / bs) AS page_count
    FROM public.pgstattuple('public.table1')
    CROSS JOIN (
        SELECT
            current_setting('block_size')::integer AS bs,
            pg_catalog.pg_relation_size(pg_catalog.pg_class.oid) AS size,
            coalesce(
                regexp_replace(
                    reloptions::text, E'.*fillfactor=(\\d+).*', E'\\1'),
                '10')::integer AS fillfactor
        FROM pg_catalog.pg_class
        WHERE pg_catalog.pg_class.oid = 'public.table1'::regclass
    ) AS const
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

SELECT DISTINCT
    indexname, tablespace, indexdef, conname,
    CASE
        WHEN conname IS NOT NULL
        THEN
            CASE
                WHEN contype = 'p'
                    THEN 'PRIMARY KEY'
                ELSE 'UNIQUE' END
        ELSE NULL END AS contypedef,
    pg_catalog.pg_relation_size(indexoid)
FROM (
    SELECT
        indexname, tablespace, indexdef,
        (
            quote_ident(schemaname) || '.' ||
            quote_ident(indexname))::regclass AS indexoid,
        string_to_array(
            regexp_replace(version(), E'.*PostgreSQL (\\d+\\.\\d+).*', E'\\1'),
            '.')::integer[] AS version
    FROM pg_catalog.pg_indexes
    WHERE
        schemaname = 'public' AND
        tablename = 'table1'
) AS sq
JOIN pg_catalog.pg_depend ON
    (
        objid = indexoid AND
        CASE
            WHEN version < array[9,1]
                THEN NOT deptype = 'i'
            ELSE true END
    ) OR (
        refobjid = indexoid AND
        NOT deptype = 'n'
    )
LEFT JOIN pg_catalog.pg_constraint ON
    conindid = indexoid AND
    contype IN ('p', 'u') AND
    conislocal
ORDER BY pg_catalog.pg_relation_size(indexoid);

SELECT
    index_size AS size,
    (100 - avg_leaf_density) - fillfactor AS free_percent,
    ceil(
        index_size::real *
        ((100 - avg_leaf_density) - fillfactor) / 100) AS free_space
FROM (
    SELECT
        index_size, avg_leaf_density,
        coalesce(
            regexp_replace(
                reloptions::text, E'.*fillfactor=(\\d+).*', E'\\1'),
            '10')::integer AS fillfactor
    FROM pg_catalog.pg_class
    CROSS JOIN (SELECT * FROM public.pgstatindex('public.table1_uidx')) AS sq
    WHERE pg_catalog.pg_class.oid = 'public.table1_uidx'::regclass
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
