@echo OFF

echo start

set "cur=%cd%"

cd %~dp0

set "wd=%cd%"

echo %wd%

rmdir %wd%\..\pike\src\main\java\com\pplive\pike\thriftgen /S /Q

%wd%\..\tools\windows\thrift-0.9.0.exe -out %wd%\..\pike\src\main\java -r --gen java %wd%\metadata.thrift

%wd%\..\tools\windows\thrift-0.9.0.exe -out %wd%\..\pike\src\main\java -r --gen java %wd%\datetransfer.thrift

cd %cur%

echo end