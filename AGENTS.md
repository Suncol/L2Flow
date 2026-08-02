# Repository Guidelines

## Project Structure & Module Organization

Public C++ headers live under `include/l2flow/`; implementations mirror domain boundaries under `src/` (`market`, `realtime`, `ipc`, `recovery`, and related modules). Production entry points are in `apps/`. C++ tests live in `tests/`, Python tests in `tests/python/`, and fuzz targets in `tests/fuzz/`. The Python client is under `python/l2flow_realtime/`, with runnable examples in `python/examples/`. Put contracts and architecture notes in `docs/`, operator utilities in `tools/`, and performance work in `benchmarks/`. Treat `build*`, `artifacts/`, and `.venv/` as generated content.

## Build, Test, and Development Commands

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The default vendor headers are read from `mdl_sdk_2_13_234/include`; override `-DL2FLOW_SDK_INCLUDE_DIR=/path/to/include` when necessary. For memory checks, configure a separate tree with `-DL2FLOW_ENABLE_ASAN_UBSAN=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo`; use `-DL2FLOW_ENABLE_TSAN=ON` separately for race detection.

## Python Virtual Environment

The repository-local `.venv` is managed with uv and must satisfy the Python
version declared in `python/pyproject.toml` (currently Python 3.10 or newer).
Activate it with `source .venv/bin/activate`; use `.venv/bin/python --version`
when an exact local interpreter version matters instead of relying on a
hard-coded documentation value. The environment contains the editable
`l2flow-realtime` package plus Polars, PyArrow, NumPy, Pandas, DuckDB, pytest,
and JupyterLab (`jupyter lab`). Contributors and agents may add, remove, or
upgrade environment packages when a task requires it, preferably with
`uv pip install --python .venv/bin/python PACKAGE`. Never commit `.venv`;
record durable project requirements in `python/pyproject.toml` instead of
relying only on local state.

## Coding Style & Naming Conventions

Use four-space indentation and follow nearby code. C++ is C++20 without extensions and must pass `-Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror`. Preserve established names: snake_case files, version suffixes such as `_v1`, PascalCase C++ types/functions, `kPascalCase` enum values, and trailing underscores for members. Python uses snake_case, type hints, and focused docstrings. No repository formatter is configured, so avoid unrelated reformatting.

## Testing Guidelines

Add focused regression tests for every behavior change. Name C++ tests `test_*.cpp` and Python tests `test_l2flow_*.py`; Python uses `unittest`, while C++ executables are registered with CTest. Prefer deterministic boundary, wire-format, lifecycle, and concurrency cases. There is no stated coverage threshold. Run the full CTest suite before review and sanitizer suites for memory- or concurrency-sensitive changes.

## Commit & Pull Request Guidelines

Prefer the history's concise, imperative Conventional Commit form: `feat(ipc): ...`, `fix(realtime): ...`, `test(python): ...`, or `docs: ...`. Keep each commit scoped. Pull requests should explain behavior and contract impact, list exact tests run, link relevant issues, and update `README.md` or `docs/` when interfaces change. Include benchmark results for latency-sensitive changes.
