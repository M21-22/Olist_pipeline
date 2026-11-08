-- Remove broad default database access.
REVOKE ALL ON DATABASE source FROM public;

-- Allow the emulator to connect and manage source tables.
GRANT CONNECT ON DATABASE source TO emulator_writer;
GRANT USAGE, CREATE ON SCHEMA public TO emulator_writer;

-- Allow the extractor to connect and read source tables.
GRANT CONNECT ON DATABASE source TO source_reader;
GRANT USAGE ON SCHEMA public TO source_reader;

-- Existing tables.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO source_reader;

-- Future tables created by emulator_writer.
ALTER DEFAULT PRIVILEGES FOR ROLE emulator_writer
IN SCHEMA public
GRANT SELECT ON TABLES TO source_reader;
