CREATE FUNCTION memcontext_crash_server()
RETURNS pg_catalog.void
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION memcontext_crash_client()
RETURNS pg_catalog.void
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION dsa_dump_sql()
RETURNS bigint
AS 'MODULE_PATHNAME' LANGUAGE C;  
