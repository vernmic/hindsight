# Switch Jev selections on. Every machine value is an explicit, overridable
# parameter (review F3); -HermesHome targets a secondary profile through HERMES_HOME.
param(
    [string]$Distro = 'Ubuntu',
    [string]$User = 'vern',
    [string]$Interpreter = '/home/vern/.hermes/hermes-agent/venv/bin/python',
    [string]$Controller = '/mnt/i/hermes/scripts/jev-control.py',
    [string]$HermesHome = ''
)
$wslArgs = @('-d', $Distro)
if ($User) { $wslArgs += @('-u', $User) }
$wslArgs += @('--')
if ($HermesHome) { $wslArgs += @('env', "HERMES_HOME=$HermesHome") }
$wslArgs += @($Interpreter, $Controller, 'on')
& wsl.exe @wslArgs
if ($LASTEXITCODE -ne 0) { throw 'Could not switch Jev selections on.' }