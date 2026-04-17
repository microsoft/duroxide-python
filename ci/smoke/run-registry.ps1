# Post-publish smoke for Windows. See run-registry.sh for details.
$ErrorActionPreference = 'Stop'

if (-not $env:DUROXIDE_VERSION) { throw "DUROXIDE_VERSION must be set" }
if (-not $env:SMOKE_SCRIPT)     { throw "SMOKE_SCRIPT must be set" }

$workdir = Join-Path $env:RUNNER_TEMP ("duroxide-smoke-" + [System.Guid]::NewGuid().ToString('N').Substring(0,8))
New-Item -ItemType Directory -Force -Path $workdir | Out-Null
Write-Host "[smoke] workdir=$workdir"
Write-Host "[smoke] version=$env:DUROXIDE_VERSION"

Push-Location $workdir
try {
  python -m venv .venv
  if ($LASTEXITCODE -ne 0) { throw "venv create failed" }
  & .\.venv\Scripts\Activate.ps1
  python -m pip install --upgrade pip | Out-Null

  $attempts = 6
  for ($i = 1; $i -le $attempts; $i++) {
    pip install --pre "duroxide==$env:DUROXIDE_VERSION"
    if ($LASTEXITCODE -eq 0) { break }
    if ($i -eq $attempts) { throw "pip install failed after $attempts attempts" }
    $sleep = $i * 10
    Write-Host "[smoke] install attempt $i failed; sleeping ${sleep}s"
    Start-Sleep -Seconds $sleep
  }

  Copy-Item $env:SMOKE_SCRIPT ./smoke.py
  python ./smoke.py
  if ($LASTEXITCODE -ne 0) { throw "python smoke.py failed ($LASTEXITCODE)" }
  Write-Host "[smoke] registry smoke OK"
} finally {
  Pop-Location
}
