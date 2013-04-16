@ECHO OFF

set argC=0
for %%x in (%*) do Set /A argC+=1
IF %argC%==0 GOTO NO_ARG 

set PATCHFILE=dev_appserver.patch
set STUBFILE=datastore_mongodb_stub.py
set SDK_GOOGLE=%1\google\
set DATASTORE_PATH=%1\google\appengine\datastore\

echo "Copying datastore mongodb stub into SDK..."
copy %STUBFILE% %DATASTORE_PATH%
copy %PATCHFILE% %SDK_GOOGLE%
cd %SDK_GOOGLE%
echo "Patching dev_appserver..."
patch --binary -p1 < %PATCHFILE%
del %PATCHFILE%
echo "Done."
GOTO END

:NO_ARG
@echo ERROR: Not enough of params
@echo ---------------------------
@echo Usage:
@echo install.bat \PATH\TO\YOUR\APPENGINE\SDK\

:END

