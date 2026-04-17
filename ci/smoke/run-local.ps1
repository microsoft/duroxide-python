# Pre-publish smoke for Windows. See run-local.sh for details.
$ErrorActionPreference = 'Stop'

if (-not $env:WHEEL_DIR)    { throw "WHEEL_DIR must be set" }
if (-not $env:SMOKE_SCRIPT) { throw "SMOKE_SCRIPT must be set" }

$workdir = Join-Path $env:RUNNER_TEMP ("duroxide-smoke-" + [System.Guid]::NewGuid().ToString('N').Substring(0,8))
New-Item -ItemType Directory -Force -Path $workdir | Out-Null
Write-Host "[smoke] workdir=$workdir"
Write-Host "[smoke] wheel-dir=$env:WHEEL_DIR"
Get-ChildItem "$env:WHEEL_DIR\*.whl"

Push-Location $workdir
try {
  python -m venv .venv
  if ($LASTEXITCODE -ne 0) { throw "venv create failed" }
  & .\.venv\Scripts\Activate.ps1

  python -m pip install --upgrade pip | Out-Null
  pip install --no-index --find-links "$env:WHEEL_DIR" duroxide
  if ($LASTEXITCODE -ne 0) { throw "pip install failed ($LASTEXITCODE)" }

  Copy-Item $env:SMOKE_SCRIPT ./smoke.py
  python ./smoke.py
  if ($LASTEXITCODE -ne 0) { throw "python smoke.py failed ($LASTEXITCODE)" }
  Write-Host "[smoke] local smoke OK"
} finally {
  Pop-Location
}
