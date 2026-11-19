# Show status, then switch Jev write gates off (write + review).
# Every machine value is an explicit, overridable parameter (review F3);
# -HermesHome targets a secondary profile through HERMES_HOME.
param(
    [string]$Distro = 'Ubuntu',
    [string]$User = 'vern',
    [string]$Interpreter = '/home/vern/.hermes/hermes-agent/venv/bin/python',
    [string]$Controller = '/mnt/i/hermes/scripts/jev-write-control.py',
    [string]$HermesHome = ''
)
$wslArgs = @('-d', $Distro)
if ($User) { $wslArgs += @('-u', $User) }
$wslArgs += @('--')
if ($HermesHome) { $wslArgs += @('env', "HERMES_HOME=$HermesHome") }
& wsl.exe @($wslArgs + @($Interpreter, $Controller, 'status'))
Write-Host ''
& wsl.exe @($wslArgs + @($Interpreter, $Controller, 'write', 'off'))
& wsl.exe @($wslArgs + @($Interpreter, $Controller, 'review', 'off'))
if ($LASTEXITCODE -ne 0) { throw 'Could not switch Jev write gates off.' }