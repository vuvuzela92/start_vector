param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"
$TaskPrefix = "StartVector"
$ConfigPath = Join-Path $PSScriptRoot "schedule.json"

if (-not (Test-Path -LiteralPath $ProjectRoot -PathType Container)) {
    throw "Каталог проекта не найден: $ProjectRoot"
}
if (-not $PythonPath) {
    $PythonPath = Join-Path $ProjectRoot "venv\Scripts\python.exe"
}
if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
    throw "Интерпретатор Python не найден: $PythonPath. Передайте путь через -PythonPath."
}

$Config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$RegistryPath = Join-Path $ProjectRoot "src_oop\tasks_registry.py"
$RegistrySource = Get-Content -LiteralPath $RegistryPath -Raw
$LauncherPath = Join-Path $PSScriptRoot "run_hidden.vbs"
$TimeoutWrapperPath = Join-Path $PSScriptRoot "run_task_with_timeout.py"
$WScriptPath = Join-Path $env:WINDIR "System32\wscript.exe"

if (-not (Test-Path -LiteralPath $LauncherPath -PathType Leaf)) {
    throw "Скрытый launcher не найден: $LauncherPath"
}
if (-not (Test-Path -LiteralPath $WScriptPath -PathType Leaf)) {
    throw "Windows Script Host не найден: $WScriptPath"
}
if (-not (Test-Path -LiteralPath $TimeoutWrapperPath -PathType Leaf)) {
    throw "Обёртка часового лимита не найдена: $TimeoutWrapperPath"
}

function New-TaskTrigger {
    <#
    .SYNOPSIS
    Создаёт триггеры Windows по расписанию бизнес-задач проекта.

    .DESCRIPTION
    Поддерживает интервальные, почасовые и ежедневные запуски выгрузок и расчётов.
    #>
    param([string]$Schedule)

    $Now = Get-Date
    switch -Regex ($Schedule) {
        '^every (5|20) minutes$' {
            $IntervalMinutes = [int]$Matches[1]
            $FirstAt = $Now.AddMinutes($IntervalMinutes - ($Now.Minute % $IntervalMinutes)).AddSeconds(-$Now.Second)
            return New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) -RepetitionDuration (New-TimeSpan -Days 30)
        }
        '^every 2 hours, starting 00:00$' {
            $FirstAt = $Now.Date.AddHours(2 * [Math]::Ceiling($Now.TimeOfDay.TotalHours / 2))
            return New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 2) -RepetitionDuration (New-TimeSpan -Days 30)
        }
        '^every 3 hours, starting 00:00$' {
            $FirstAt = $Now.Date.AddHours(3 * [Math]::Ceiling($Now.TimeOfDay.TotalHours / 3))
            return New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 3) -RepetitionDuration (New-TimeSpan -Days 30)
        }
        '^daily at (\d{2}):(\d{2})$' {
            return New-ScheduledTaskTrigger -Daily -At $Now.Date.AddHours([int]$Matches[1]).AddMinutes([int]$Matches[2])
        }
        '^daily at (\d{2}):(\d{2}) and (\d{2}):(\d{2})$' {
            $Times = @(
                $Now.Date.AddHours([int]$Matches[1]).AddMinutes([int]$Matches[2]),
                $Now.Date.AddHours([int]$Matches[3]).AddMinutes([int]$Matches[4])
            )
            return @($Times | ForEach-Object {
                $FirstAt = $_
                if ($FirstAt -le $Now) { $FirstAt = $FirstAt.AddDays(1) }
                New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Days 1) -RepetitionDuration (New-TimeSpan -Days 3650)
            })
        }
        '^hourly at minute (\d{2}) and (\d{2})$' {
            $Minutes = @([int]$Matches[1], [int]$Matches[2])
            return @($Minutes | ForEach-Object {
                $FirstAt = $Now.Date.AddHours($Now.Hour).AddMinutes($_)
                if ($FirstAt -le $Now) { $FirstAt = $FirstAt.AddHours(1) }
                New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 30)
            })
        }
        '^hourly at minute (\d{2}, \d{2}, \d{2}, \d{2})$' {
            $Minutes = $Matches[1] -split ', ' | ForEach-Object { [int]$_ }
            return @($Minutes | ForEach-Object {
                $FirstAt = $Now.Date.AddHours($Now.Hour).AddMinutes($_)
                if ($FirstAt -le $Now) { $FirstAt = $FirstAt.AddHours(1) }
                New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 30)
            })
        }
        '^hourly at minute (\d{2}) and (\d{2}) and (\d{2}) and (\d{2})$' {
            $Minutes = @([int]$Matches[1], [int]$Matches[2], [int]$Matches[3], [int]$Matches[4])
            return @($Minutes | ForEach-Object {
                $FirstAt = $Now.Date.AddHours($Now.Hour).AddMinutes($_)
                if ($FirstAt -le $Now) { $FirstAt = $FirstAt.AddHours(1) }
                New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 30)
            })
        }
        '^hourly at minute (\d{2})$' {
            $FirstAt = $Now.Date.AddHours($Now.Hour).AddMinutes([int]$Matches[1])
            if ($FirstAt -le $Now) { $FirstAt = $FirstAt.AddHours(1) }
            return New-ScheduledTaskTrigger -Once -At $FirstAt -RepetitionInterval (New-TimeSpan -Hours 1) -RepetitionDuration (New-TimeSpan -Days 30)
        }
        default { throw "Неподдерживаемое расписание: $Schedule" }
    }
}

foreach ($Task in $Config.tasks) {
    $TaskRoot = if ($Task.project_root) { $Task.project_root } else { $ProjectRoot }
    $TaskPython = if ($Task.python_path) { $Task.python_path } else { $PythonPath }
    if ($Task.script) {
        $TaskScript = Join-Path $TaskRoot $Task.script
    } else {
        $TaskScript = Join-Path $TaskRoot "main.py"
        $RegistryPattern = '(?m)^\s*"' + [regex]::Escape($Task.name) + '"\s*:'
        if ($RegistrySource -notmatch $RegistryPattern) {
            throw "Задача '$($Task.name)' отсутствует в реестре: $RegistryPath"
        }
    }
    if (-not (Test-Path -LiteralPath $TaskRoot -PathType Container)) {
        throw "Каталог проекта задачи не найден: $TaskRoot"
    }
    if (-not (Test-Path -LiteralPath $TaskPython -PathType Leaf)) {
        throw "Интерпретатор Python задачи не найден: $TaskPython"
    }
    if (-not (Test-Path -LiteralPath $TaskScript -PathType Leaf)) {
        throw "Файл запуска задачи не найден: $TaskScript"
    }

    $ScriptArguments = if ($Task.script_arguments) { $Task.script_arguments } elseif ($Task.script) { "" } else { $Task.name }
    $LauncherArguments = '"' + $LauncherPath + '" "' + $TaskPython + '" "' + $TaskRoot + '" "' + $TaskScript + '" "' + $ScriptArguments + '" "' + $TimeoutWrapperPath + '"'
    $Action = New-ScheduledTaskAction -Execute $WScriptPath -Argument $LauncherArguments -WorkingDirectory $TaskRoot
    $Trigger = New-TaskTrigger -Schedule $Task.schedule
    $Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 1)
    $Name = "$TaskPrefix-$($Task.name)"
    Register-ScheduledTask -TaskName $Name -Action $Action -Trigger $Trigger -Settings $Settings -Description "Запуск задачи Start Vector: $($Task.name)" -Force | Out-Null
    Write-Host "Зарегистрирована задача: $Name ($($Task.schedule))"
}

Write-Host "Готово. Зарегистрировано задач: $($Config.tasks.Count). Часовой пояс Windows: $((Get-TimeZone).Id)"

