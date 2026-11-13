& wsl -d Ubuntu -- /home/vern/.hermes/hermes-agent/venv/bin/python /home/vern/.hermes/scripts/jev-write-control.py status
Write-Host ""
& wsl -d Ubuntu -- /home/vern/.hermes/hermes-agent/venv/bin/python /home/vern/.hermes/scripts/jev-write-control.py write off
& wsl -d Ubuntu -- /home/vern/.hermes/hermes-agent/venv/bin/python /home/vern/.hermes/scripts/jev-write-control.py review off
