::  AUTHOR pokuam1                                          :
::  ORIGINAL DATE 09/27/12                                  : 
::                                                          :
:::Explanation of Directory Structures ::::::::::::::::::::::
:: 
:: CSV files are pulled off Android phone into this directory
::    C:\PATH_TO_ROOT_SMS_DIR\RAPIDANDROID_OUTPUT\
::
:: .sh and .bat scripts that process the CSV files go here
::    C:\PATH_TO_ROOT_SMS_DIR\RAPIDANDROID_OUTPUT\scripts\
::
:: processed CSV files go here and are ingested by the ETL jar module
::    C:\PATH_TO_ROOT_SMS_DIR\RAPIDANDROID_OUTPUT\loadingzone\
::
:: location of the ETL jar module
::    C:\PATH_TO_ROOT_SMS_DIR\sages-etl-1.0-SNAPSHOT-bin\


set SCRIPTNAME=backup4sms
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
@echo starting %SCRIPTNAME% script...
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"


set CURDIR=%CD%

set CURDATE=%date:~-4,4%%date:~-7,2%%date:~-10,2%
set tmp_time=%time:~-11,2%%time:~-8,2%%time:~-5,2%
:: method 1 use this to trim leading space entirely
::set tmp_time=%tmp_time: =%

:: method 2 use this to replace leading space with 0
set tmp_time=%tmp_time: =0%

set CURTIME=%tmp_time%
@echo %CURDATE%_%CURTIME%

:: execute .sh script to copy csv files from source Android device to destination machine
:sh pullCsvAndDiff.sh

:cd ..\..\..\..\sages-etl-2.0-SNAPSHOT-module\
cd ..\..\..\..\dev\

mkdir BACKUPS4SMS
mkdir BACKUPS4SMS\%CURDATE%_%CURTIME%

adb pull sdcard/rapidandroid/rapidandroid.db BACKUPS4SMS\%CURDATE%_%CURTIME%
adb pull sdcard/rapidandroid/logs/sages_system_health.log BACKUPS4SMS\%CURDATE%_%CURTIME%

::cd C:\SAGES_CLINIC_MORBIDITY_VISITS\sages-etl-1.0-SNAPSHOT-bin\
::java -jar C:\SAGES_CLINIC_MORBIDITY_VISITS\sages-etl-1.0-SNAPSHOT-bin\sages-etl-1.0-SNAPSHOT-sages-consumer-jar-with-dependencies.jar

cd %CURDIR%

@echo.
@echo.
@echo. 
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
@echo completed running %SCRIPTNAME% script.
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"