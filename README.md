# projects-erdera

MOLGENIS implementation for ERDERA

## Getting started

### Expressions

For the MOLGENIS-RD3 implementation for ERDERA, the expressions are located in at `./src/js/*`. Files are named using the following format: `<table>_<column>_<expressionType>`. Expressions are also documented within the scripts an overview is generated ([docs/expressions/README.md](./docs/expressions/README.md)). Expressions also have unit tests to ensure the functions work as expected (these are run in isolation from the EMX2 instance).

```cli
# run unit tests
yarn test


# build and lint docs
yarn docs
```
