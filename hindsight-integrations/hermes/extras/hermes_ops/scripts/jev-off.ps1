& wsl.exe -d Ubuntu -- /home/vern/.hermes/hermes-agent/venv/bin/python /mnt/i/hermes/scripts/jev-control.py off
if ($LASTEXITCODE -ne 0) { throw 'Could not switch Jev selections off.' }
