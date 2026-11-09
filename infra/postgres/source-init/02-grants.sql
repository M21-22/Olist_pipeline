-- Remove broad default database access.
REVOKE ALL ON DATABASE source FROM public;

-- Emulator database access.
GRANT CONNECT ON DATABASE source TO emulator_writer;
GRANT USAGE, CREATE ON SCHEMA shop TO emulator_writer;
GRANT USAGE, CREATE ON SCHEMA chaos TO emulator_writer;

-- Extractor database access.
GRANT CONNECT ON DATABASE source TO source_reader;
GRANT USAGE ON SCHEMA shop TO source_reader;

-- Read access to existing operational tables.
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO source_reader;

-- Automatically grant read access to future operational tables.
ALTER DEFAULT PRIVILEGES FOR ROLE emulator_writer
IN SCHEMA shop
GRANT SELECT ON TABLES TO source_reader;

-- Explicitly prevent extractor access to emulator internals.
REVOKE ALL ON SCHEMA chaos FROM public;
REVOKE ALL ON SCHEMA chaos FROM source_reader;
