$ErrorActionPreference = 'Stop'
# Configuration only: no host path, user, distro or interpreter is compiled in.
# HERMES_HOME is required because a WSL path cannot be derived from a Windows
# profile. Everything else falls back to the documented layout below.
$HermesHome = $env:HERMES_HOME
if (-not $HermesHome) { throw 'Set HERMES_HOME to the WSL Hermes home (for example /home/<user>/.hermes).' }
$Workspace = if ($env:HERMES_OPS_WORKSPACE) { $env:HERMES_OPS_WORKSPACE } else { "$HermesHome/workspace" }
$Python = if ($env:HERMES_OPS_PYTHON) { $env:HERMES_OPS_PYTHON } else { "$HermesHome/venvs/ops/bin/python" }
$Distro = if ($env:HERMES_WSL_DISTRO) { $env:HERMES_WSL_DISTRO } else { 'Ubuntu' }
$Controller = if ($env:HERMES_OPS_JEV_WRITE_CONTROL) { $env:HERMES_OPS_JEV_WRITE_CONTROL } else { "$Workspace/scripts/jev-write-control.py" }
& wsl.exe -d $Distro -e $Python $Controller status
Write-Host ""
& wsl.exe -d $Distro -e $Python $Controller write off
& wsl.exe -d $Distro -e $Python $Controller review off
