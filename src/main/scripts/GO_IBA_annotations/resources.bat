@echo off
cd %1
@REM for %%f in (%2\*.gaf.gz) do (
@REM     echo Downloading %%f
@REM     powershell -Command "Invoke-WebRequest -Uri '%%f' -OutFile '%%~nxf'"
@REM )
@REM echo Unzipping gaf files...
@REM for %%f in (*.gaf.gz) do (
@REM     powershell -Command "Expand-Archive -Path '%%f' -DestinationPath ."
@REM     del "%%f"
@REM )
@REM echo Finished unzipping gaf files.
echo Downloading obo file...
powershell -Command "Invoke-WebRequest -Uri '%3' -OutFile '%~nx3'"
echo Finished downloading obo file. 