@echo off

:: 관리자 권한을 획득합니다.
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
start cmd /k %KAFKA_HOME%\bin\windows\kafka-server-stop.bat

TIMEOUT 3

start cmd /k %KAFKA_HOME%\bin\windows\zookeeper-server-stop.bat

TIMEOUT 3

:: Kafka의 모든 로그파일을 삭제합니다.
rmdir /s /Q %KAFKA_HOME%\data\zookeeper\version-2
rmdir /s /Q %KAFKA_HOME%\data\kafka
rmdir /s /Q %KAFKA_HOME%\logs