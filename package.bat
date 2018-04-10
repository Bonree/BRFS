@echo off

@rem 设置文件目录
set dirs=brfs\bin,brfs\jar,brfs\config,brfs\logs,brfs\lib
@rem 设置当前路径
set baseDir=%~dp0
set version=%date:~0,4%%date:~5,2%%date:~8,2%
set keyword="Last Changed Rev"
for /f "delims=" %%i in ('svn info %baseDir% ^| findstr /C:%keyword%') do set rev=%%i
set svn_version=%rev:~18%

@rem 设置需要打包的模块
set modules=FS_Client,FS_Server

@rem 初始项目的目录结构
rd /s /q brfs
for %%i in (%dirs%) do (
	if exist %baseDir%%%i (
		echo dir '%%i' already exist.
	) else (
		md %%i
		if "%%i" == "brfs\jar" (
			for %%n in (%modules%) do md %%i\%%n
		)
		echo dir '%%i' not exist, create it.
	)
)

@rem 工程里生成版本号文件
for %%i in (%modules%) do if exist %%i\src\main\resources\ver_*.txt del /s /q %%i\src\main\resources\ver_*.txt
for %%i in (%modules%) do echo version: %version%  svn-version: %svn_version% >  %%i\src\main\resources\ver_%version%.txt

@rem maven打包
call mvn clean package -Dmaven.test.skip=true

@rem 删除工程里版本号文件
for %%i in (%modules%) do if exist %%i\src\main\resources\ver_*.txt del /s /q %%i\src\main\resources\ver_*.txt

@rem 复制程序jar,配置文件,启动脚本,版本文件
for %%i in (%modules%) do copy %%i\target\%%i.jar brfs\jar\%%i\

copy config\*.* brfs\config\
copy bin\*.* brfs\bin\
copy lib\*.* brfs\lib\
copy release\*.* brfs\
echo %version% > brfs\ver_%version%.txt

@rem 打zip包
for %%i in (%modules%) do  7z.exe a -tzip sdk\zip\%%i.zip %baseDir%\sdk\jar\%%i\* %baseDir%\sdk\readme.txt %baseDir%\sdk\release-notes.txt

pause
