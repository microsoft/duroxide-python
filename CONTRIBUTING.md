# Contributing

This project welcomes contributions and suggestions. Most contributions require you to
agree to a Contributor License Agreement (CLA) declaring that you have the right to,
and actually do, grant us the rights to use your contribution. For details, visit
https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need
to provide a CLA and decorate the PR appropriately (for example, label or comment).
Simply follow the instructions provided by the bot. You will only need to do this once
across all repositories using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Reporting security issues

Please do not report security vulnerabilities through public GitHub issues. Follow the instructions in [SECURITY.md](SECURITY.md).

## Development workflow

Before opening a pull request, run the checks relevant to your change:

```bash
source .venv/bin/activate
maturin develop
cargo clippy --all-targets
pytest -v
```

After Rust source changes (`src/*.rs`), re-run `maturin develop` before running Python tests.

### Lifecycle checks without PostgreSQL

Use an isolated virtual environment with the declared development dependencies.
Rebuild/install the extension for each mode; importing an old extension is not
lifecycle evidence. For example, in PowerShell with that environment activated:

```powershell
maturin develop --features test-hooks
$env:DUROXIDE_LIFECYCLE_TEST_HOOKS = '1'
python -m pytest -v tests\test_lifecycle.py tests\test_e2e.py::test_sqlite_smoketest
Remove-Item Env:DUROXIDE_LIFECYCLE_TEST_HOOKS
maturin develop
python -m pytest -v tests\test_lifecycle.py tests\test_e2e.py::test_sqlite_smoketest
```

Instrumented cases exercise real provider waits and contained owned-task faults.
The suite checks native import provenance, 100 repetitions per ordered race,
bounded child-process cleanup, and production export absence. Production mode
skips only hook-dependent cases. Do not publish instrumented assets or temporary
local core overrides. A matching published core minimum is required before release.