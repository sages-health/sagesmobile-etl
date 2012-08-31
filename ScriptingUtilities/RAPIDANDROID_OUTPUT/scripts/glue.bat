::  AUTHOR pokuam1  		  							    :
::  ORIGINAL DATE 1/09/11       						    : 
::  REFACTOR DATE 12/06/11	    							:
:: 															:
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


set SCRIPTNAME=glue
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
@echo starting %SCRIPTNAME% script...
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"


set CURDIR=%CD%

:: execute .sh script to copy csv files from source Android device to destination machine
sh pullCsvAndDiff.sh

cd ..\..\sages-etl-1.0-SNAPSHOT-bin\
java -jar sages-etl-1.0-SNAPSHOT-sages-consumer-jar-with-dependencies.jar

::cd C:\SAGES_CLINIC_MORBIDITY_VISITS\sages-etl-1.0-SNAPSHOT-bin\
::java -jar C:\SAGES_CLINIC_MORBIDITY_VISITS\sages-etl-1.0-SNAPSHOT-bin\sages-etl-1.0-SNAPSHOT-sages-consumer-jar-with-dependencies.jar

cd %CURDIR%

@echo.
@echo.
@echo. 
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
@echo completed running %SCRIPTNAME% script.
@echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"