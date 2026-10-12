# Windows PowerShell equivalent of lab.sh. Examples:
#   .\scripts\lab.ps1 step all --reset
#   .\scripts\lab.ps1 step 1
#   .\scripts\lab.ps1 verify
$ErrorActionPreference = 'Stop'
$labArguments = @($args)
if ($labArguments.Count -eq 0) {
    Write-Host 'Usage: .\scripts\lab.ps1 <step|exp|verify|test|run|cli|dbt|shell> [arguments]'
    exit 1
}
$labCommand = $labArguments[0]
$labRemaining = @($labArguments | Select-Object -Skip 1)
$labPython = '/home/airflow/lab-venv/bin/python'
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    switch ($labCommand) {
        { $_ -in 'step', 'exp', 'verify' } {
            & docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli $labCommand @labRemaining
        }
        'cli' { & docker compose exec -T -w /opt/lab airflow $labPython -m pipeline.cli @labRemaining }
        'test' { & docker compose exec -T -w /opt/lab airflow $labPython -m unittest discover -s tests }
        'run' { & docker compose exec -T -w /opt/lab airflow bash scripts/run_pipeline.sh @labRemaining }
        'dbt' { & docker compose exec -T -w /opt/dbt airflow /home/airflow/lab-venv/bin/dbt @labRemaining --no-use-colors }
        'shell' { & docker compose exec airflow bash }
        default { throw "Unknown lab command: $labCommand" }
    }
    $labExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $labExitCode
