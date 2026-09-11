-- Creates the retrieval_service database on first-ever init of the shared
-- graphgpt-postgres data volume (docker-entrypoint-initdb.d scripts only run
-- once, against an empty data directory).
--
-- If the shared postgres_data volume already existed before this file was
-- added (e.g. it was first created for auth-service's auth_db / file-service's
-- file_service_db), this script will NOT run automatically. In that case use
-- the idempotent fallback:
--   make -C retrieval-service db-create
-- which runs the equivalent CREATE DATABASE via `docker exec` against the
-- already-running container.
SELECT 'CREATE DATABASE retrieval_service_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'retrieval_service_db')\gexec
