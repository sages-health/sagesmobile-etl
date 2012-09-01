Steps to Patch SMS scripts & Reconfigure System
Dec 14, 2011

Delete old files from RAPIDANDROID_OUTPUT 
--------------------------------------------
1. Go to SAGES_CLINIC_MORBIDITY_VISITS\sages-consumer-11062011-1315\RAPIDANDROID_OUTPUT
2. Delete everything in this directory. This will include:
   - all formdata_*.csv files
   - all *.bat, *.sh files
   - all subdirectories such as "loadingzone" and "done"

   
Add new files to RAPIDANDROID_OUTPUT 
--------------------------------------------  
3. Copy the new subdirectory "scripts" in the RAPIDANDROID_OUTPUT directory


Download new Unix utilities for Windows (MSYS = Minimalist GNU for Windows)
---------------------------------------------------------------------------
4. Obtain the MSYS.exe download
   - download link: http://downloads.sourceforge.net/mingw/MSYS-1.0.11.exe
   - for convenience, we have provided the executable, see the bundle we've provided


Install new Unix utilities for Windows (MSYS = Minimalist GNU for Windows)
--------------------------------------------------------------------------
5. Double-click MSYS-1.0.11.exe and install in your desired location
6. Add MSYS's bin direcotry to the System Path
   - add new variable MSYS_HOME, set value to where you installed MSYS (for example, C:\msys\1.0)
   - add %MSYS_HOME%\bin to your system Path variable

  
Verify that MSYS's bin is recognized on the Path
------------------------------------------------
7. Try to run some of the MSYS utilities at the command line
  - ls, pwd, cls, touch

  
Verify the Android phone is attached to the computer and visible
----------------------------------------------------------------
8. Try to run 'adb devices' at the command line. Verify that the device serial number
   is displayed at the command line. 
  

Bootstrap the SMS setup
-----------------------
- A system being run from a fresh start needs to be "bootstrapped" which ensures 2 things:
	 1) The first csv data file from the phone is copied to the location for being loaded into the database
	 2) The first csv data file from the phone is left in the location for being "diffed" against subsequent
	   data files pulled from the phone. "Diff" is a process of comparing two files for changes

- The "bootstrap" step is done once, and the assumption is the system will then be run in "normal" mode.

9. To "bootstrap" the system
   Assumptions:
    * Android device is attached to the computer
    * RapidAndroid has data to output (Manually output the CSV file from the user interface)

    Steps:
	1. Manually output the data as a csv file through the RapidAndroid UI
	2. From the command line, navigate to RAPIDANDROID_OUTPUT directory and run: sh pullCsvAndDiff.sh -m boostrap
	3. Verfiy that the csv file is in the RAPIDANDROID_OUTPUT directory and RAPIDANDROID_OUTPUT\loadingzone directory. Note  
		that its name will have been changed in the loadingzone directory with a timestamp of when the script ran.
	4. Proceed to setup the system on "normal" mode on Windows Task Scheduler
	   
	   
Configure scripts to run on Windows Task Scheduler
--------------------------------------------
10. Start > Administrative Tools > Task Scheduler
11. On right side menu choose "Create Task..."
12. Fill out the Tabbed Panels (General, Triggers and Actions)
13. General:
	Name: SMS-ETL
14. Triggers:
    Select "New..." button
	Configure your desired schedule
15. Actions:
	Select "New..." button
	Action: leave set at "Start a program"
	Program/script: Hit "Browse..." button and select, 
					C:\PATH_TO_SMS_ROOT\RAPIDANDROID_OUTPUT\scripts\glue.bat
					(NOTE: PATH_TO_SMS_ROOT ~= SAGES_CLINIC_MORBIDITY_VISITS\sages-consumer-11062011-1315)
					
	IMPORTANT => Start in: C:\PATH_TO_SMS_ROOT\RAPIDANDROID_OUTPUT\scripts\
16. Enable the task and start it
