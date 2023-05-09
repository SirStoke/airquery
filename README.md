# Airquery

Run SQL queries directly on Airtable bases, without copying data into an RDBMS first!

# Install

## Using docker

You can use [stoke/airquery](https://hub.docker.com/r/stoke/airquery):

```
docker run --rm stoke/airquery
```

## Using nix

This repository includes a flake, just run:

```
nix run github:SirStoke/airquery
```

# How to use

Requirements:

- [Find your Base ID](https://support.airtable.com/docs/finding-airtable-ids#finding-ids-in-airtable-api)
- [Create a Personal Access Token](https://support.airtable.com/docs/creating-and-using-api-keys-and-access-tokens#personal-access-tokens-basic-actions)

Then, you can run `airquery` like so: 

```
airquery -k <PERSONAL_ACCESS_TOKEN> -b <BASE_ID> <query>
```

The underlying base can be accessed under `airtable.airtable.<lowercase-base-name>`. E.g.

```
airquery -k <PERSONAL_ACCESS_TOKEN> -b <BASE_ID> 'SELECT * FROM airtable.airtable.transactions'
```

For more advanced usage, just follow `airquery --help` (`cargo run -- --help` from the source).

# SQL support

Under the hood, airquery uses [datafusion](https://arrow.apache.org/datafusion/) as its SQL engine. You can check out their [SQL Reference](https://arrow.apache.org/datafusion/user-guide/sql/index.html) to know which features are supported.

# TODO

## Data types

- [ ] Support [basic airtable types](https://airtable.com/developers/web/api/field-model)
  - [x] singleLineText / singleSelect / phoneNumber / email
  - [x] currency / number / percent
  - [x] date
  - [ ] dateTime
  - [ ] checkbox (treated as number, either 0 or 1. No support for native booleans in datafusion)


- [ ] Support [advanced airtable types](https://airtable.com/developers/web/api/field-model)
  - [ ] duration
  - [ ] formula
  - [ ] lookup
  - [ ] multiple select
  - [ ] long text / rich text

## Features

- [ ] SQL engine
  - [x] Basic SQL support
  - [ ] Filters pushdown
  - [ ] Sorting pushdown

- [x] Basic CLI
  - [x] JSON output
  - [x] Prettified tables

- [ ] WASM support
  - [ ] Can run inside an [airtable script](https://airtable.com/developers/scripting)
