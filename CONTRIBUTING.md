# Contributing

Thanks for your interest in improving this project. Please follow the rules below so that
reviews stay fast and the code base stays homogeneous.

## Build & Test

Before submitting a change, verify that everything passes locally:

```sh
make CROSS_COMPILE= MODE=host     # host build
make test CROSS_COMPILE=          # unit tests (host)
make lint                         # cppcheck static analysis (if installed)
make docs                         # Doxygen documentation (if installed)
```

CI runs the same checks on every push; a merge request is only accepted when CI is green.

## Coding Style

- C11, 4-space indentation, no tabs in sources (tabs are only used in the Makefile recipes)
- Allman brace style, one statement per line
- Every function gets a banner comment (`@brief`, `@param[in/out]`, `@return`) in English;
  comments are written for Doxygen
- No dynamic allocations on the hot path unless the existing pattern already does so
- New public functions must be declared in the module header and documented there
- Format with the provided `.clang-format` / `.editorconfig` before committing

## Commits

- Use concise, imperative commit subjects (`Add QoS2 support`, not `added qos2`)
- Reference issues in the body where applicable (`Closes #12`)

## Changes

- User-visible changes must be added to `CHANGELOG.md` under `[Unreleased]`
- Breaking API changes require a major version bump and a migration note in the changelog

## Tests

- New features in `src/broker` or `src/util` should come with unit tests in `tests/`
- Bug fixes should include a regression test where practical
