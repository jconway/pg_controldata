/* $PostgreSQL$ */

-- Adjust this setting to control where the objects get dropped.
SET search_path = public;

DROP VIEW pg_controldata;
DROP FUNCTION pg_controldata();
DROP FUNCTION pg_controldata_reset();
