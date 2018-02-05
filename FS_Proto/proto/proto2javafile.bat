@echo off
FOR %%p in (*.proto) do protoc.exe  --java_out=..\src\main\java\ %%p
pause