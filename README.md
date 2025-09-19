# duckdb_mooncake
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FMooncake-Labs%2Fduckdb_mooncake.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FMooncake-Labs%2Fduckdb_mooncake?ref=badge_shield)


duckdb_mooncake is a DuckDB extension to read Iceberg tables written by [moonlink][moonlink-link] in real time.

## Installation

duckdb_mooncake can be installed using the `INSTALL` command:
```sql
INSTALL duckdb_mooncake FROM community;
```

## Usage

Mooncake databases can be attached using the `ATTACH` command, after which tables can be queried using standard SQL.

The example below attaches to the moonlink database `'postgres'`, from a moonlink instance listening at `'/var/lib/postgresql/data/pg_mooncake/moonlink.sock'`. This moonlink instance comes prepopulated with a table named `public.c`:
```sql
D ATTACH DATABASE 'mooncake' (TYPE mooncake, URI '/var/lib/postgresql/data/pg_mooncake/moonlink.sock', DATABASE 'postgres');
D SELECT * FROM mooncake.public.c;
┌───────┬─────────┐
│  id   │   val   │
│ int32 │ varchar │
├───────┼─────────┤
│     1 │ Hello   │
│     2 │ World   │
└───────┴─────────┘
```

## Building

To build, type:
```
git submodule update --init --recursive
GEN=ninja make
```

To run, run the bundled `duckdb` shell:
```
./build/release/duckdb
```

[moonlink-link]: https://github.com/Mooncake-Labs/moonlink


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FMooncake-Labs%2Fduckdb_mooncake.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FMooncake-Labs%2Fduckdb_mooncake?ref=badge_large)