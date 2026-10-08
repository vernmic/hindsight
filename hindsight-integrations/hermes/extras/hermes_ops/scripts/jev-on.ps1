& wsl.exe -d Ubuntu -- /home/vern/.hermes/hermes-agent/venv/bin/python /mnt/i/hermes/scripts/jev-control.py on
if ($LASTEXITCODE -ne 0) { throw 'Could not switch Jev selections on.' }
