@echo off

:: ��ũ��Ʈ ������ �����ڱ����� ���մϴ�.
:-------------------------------------
REM --> Check for permissions
>nul 2>&1 "%SYSTEMROOT%\system32\cacls.exe" "%SYSTEMROOT%\system32\config\system"
REM --> If error flag set, we do not have admin.
if '%errorlevel%' NEQ '0' (
	echo Requesting administrative privileges...
	goto UACPrompt
) else ( goto gotAdmin )

:UACPrompt
	echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
	set params = %*:"=""
	echo UAC.ShellExecute "cmd.exe", "/c %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs"

	"%temp%\getadmin.vbs"
	del "%temp%\getadmin.vbs"
	exit /B

:gotAdmin
	pushd "%CD%"
	CD /D "%~dp0"
:--------------------------------------
REM --> �ϵ� YARN�� �����մϴ�.
start cmd /k %HADOOP_HOME%\sbin\stop-yarn.cmd

REM --> �ϵ� ���� �ý����� �����մϴ�.
start cmd /k %HADOOP_HOME%\sbin\stop-dfs.cmd