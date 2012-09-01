Steps to Run the Scripts
Dec 15, 2011

There are 3 modes that the pullCsvAndDiff.sh script will run in:
1) Normal - during live system when no human intervention is needed and assumes system was already "bootstrapped"
		    
			to run: sh pullCsvAndDiff.sh

2) Force - ** for development use** when you want to force all files currently in RAPIDANDROID_OUTPUT directory to be 	   
            processed, note all CSV files are removed subsequently, so do not use this if expecting to run in Normal mode. If 
			you do want to run in Normal mode again, you will have to bootstrap the setup. To get back into the “normal” state 
			of operation, “bootstrap” once from the command line and then turn on the Windows Task Scheduler (which assumedly 
			points to a script that is hard wired to run in “normal” mode). 
		   
		    to run: sh pullCsvAndDiff.sh -m force
			
3) Bootstrap - used to process the first CSV file in a new setup, but also leave it in RAPIDANDROID_OUTPUT for subsequent runs
               in "normal" mode. 
			
			to run: sh pullCsvAndDiff.sh -m boostrap
			
Instructions for setting up System to run in Normal Mode
1. Setup the directories as described in README_stepsToPatch.txt
2. Connect the android device and manually run at command line:
       sh pullCsvAndDiff.sh -m bootstrap at the command line	
3. Then enable the Windows Task Scheduler and let it run (note the glue.bat file calls pullCsvAndDiff.sh in the "normal" mode)

