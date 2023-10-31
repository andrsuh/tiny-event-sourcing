create schema event_sourcing_store;
create table if not exists event_sourcing_store.event_record (
       id text primary key default gen_random_uuid(),
       aggregate_table_name text,
       aggregate_id text,
       aggregate_version bigint,
       event_title text,
       payload text,
       saga_context text,
       createdAt bigint
);

create table if not exists event_sourcing_store.snapshot (
    id text primary key,
    snapshot_table_name text,
    snapshot text,
    version bigint
);

create table if not exists event_sourcing_store.event_stream_read_index (
    id text primary key,
    read_index bigint,
    version bigint
);

create table if not exists event_sourcing_store.event_stream_active_readers (
    id text primary key,
    version bigint,
    reader_id text,
    read_position bigint,
    last_interaction bigint
)