Option Explicit

' Запускает одну задачу без консольного окна; журнал ведёт приложение.
Dim shell, pythonPath, projectRoot, scriptPath, scriptArguments, wrapperPath, commandLine
Set shell = CreateObject("WScript.Shell")

If WScript.Arguments.Count <> 5 Then
    WScript.Quit 2
End If

pythonPath = WScript.Arguments(0)
projectRoot = WScript.Arguments(1)
scriptPath = WScript.Arguments(2)
scriptArguments = WScript.Arguments(3)
wrapperPath = WScript.Arguments(4)
shell.CurrentDirectory = projectRoot
commandLine = QuoteArgument(pythonPath) & " " & QuoteArgument(wrapperPath) & " " & QuoteArgument(pythonPath) & " " & QuoteArgument(scriptPath)
If Len(scriptArguments) > 0 Then
    commandLine = commandLine & " " & scriptArguments
End If
shell.Run commandLine, 0, True
WScript.Quit 0

Function QuoteArgument(value)
    QuoteArgument = Chr(34) & Replace(value, Chr(34), Chr(34) & Chr(34)) & Chr(34)
End Function
