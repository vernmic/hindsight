param([ValidateSet('extractor','gardener')][string]$Component='extractor',[ValidateSet('on','off','status')][string]$Mode='status')
& wsl.exe -d Ubuntu -u vern -- /home/vern/.hermes/hermes-agent/venv/bin/python /mnt/i/hermes/scripts/memory-quality-control.py $Component $Mode
if ($LASTEXITCODE -ne 0) { throw 'Memory quality control failed' }
