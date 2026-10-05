<#
  PowerShell equivalent of the Makefile targets (Windows without make).

  .\scripts\lab.ps1 install | up | down | setup | seed | stream | lint | test | lab-sql | lab-spark | lab
#>
param([Parameter(Mandatory = $true)][string]$Target)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Run([string]$cmd) {
  Write-Host ">> $cmd" -ForegroundColor Cyan
  Invoke-Expression $cmd
  if ($LASTEXITCODE -ne 0) { throw "failed: $cmd" }
}

switch ($Target) {
  "install"   { Run "uv sync" }
  "up"        { Run "docker compose up -d"; Run "docker compose ps" }
  "down"      { Run "docker compose down" }
  "setup"     { Run "uv run python -m lab.workload setup" }
  "seed"      { Run "uv run python -m lab.workload seed --customers 500 --orders 2000" }
  "stream"    { Run "uv run python -m lab.workload stream --tps 5 --duration 600" }
  "lint"      {
    Run "uv run ruff check"
    Run "uv run ruff format --check"
    Run "uv run mypy"
  }
  "test"      { Run "uv run pytest -q" }
  "lab-sql"   {
    Run "uv run python -m lab.checks.t2_timezone"
    Run "uv run python -m lab.checks.t3_read_semantics"
    Run "uv run python -m lab.checks.t4_watermark_concurrency"
    Run "uv run python -m lab.checks.t1_idle_heartbeat"
  }
  "lab-spark" {
    Run "uv run python -m lab.checks.t5_engine"
    Run "uv run python -m lab.checks.t6_delta_semantics"
    Run "uv run python -m lab.checks.t7_end_to_end --idle-minutes 6"
    Run "uv run python -m lab.checks.t9_capture_instance_switch"
    Run "uv run python -m lab.checks.t10_chunked_snapshot"
    Run "uv run python -m lab.checks.t10_chunked_snapshot --resnapshot"
  }
  "lab"       { foreach ($t in "setup", "seed", "lab-sql", "lab-spark") { & $PSCommandPath $t } }
  default     { throw "unknown target: $Target" }
}
