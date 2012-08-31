#!/bin/bash
#################################
# AUTHOR pokuam1  		        #
# DATE 01/08/11			        #
# REFACTORED 12/02/11  		    #
#################################
# Dev Notes:
# 
#
# To suppress output use:
#    &> /dev/null 
# - stderror would cause Windows 2008 Server R2 to launch an alert box
# - stdout and console sometimes too cluttered with output
#
# To see an exit code:
#    echo "exitcode=" $?
#################################
# Imported script files:

  . func_diff.sh
#################################

START="---------------Starting pull csv and diff--------------------"
echo ""
echo ""
echo ""
echo "-------------------------------------------------------------"
echo $START
echo "-------------------------------------------------------------"
echo ""

#####################################################################
# DETERMINE FLAG FROM COMMANDLINE OPTIONS 
# (FLAG=bootstrap|force)
#####################################################################

FLAG=
destFlag="../.."

NO_ARGS=0
E_OPTERROR=85

#if [ $# -eq "$NO_ARGS" ] # Script invoked with no command-line args?
#  then
#    echo "Usage: `basename $0` options -m[force|bootstrap| or leave off])"
#    exit $E_OPTERROR # Exit and explain usage.
#    # Usage: scriptname -options
#    # Note: dash (-) necessary
#fi

while getopts "m:" Option
  do
    case $Option in
	#p ) #echo "selected option -p- with argument \"$OPTARG\" [OPTIND=${OPTIND}]"
	#    destFlag=$OPTARG
	#    ;;
	m ) #echo "selected option -m- with argument \"$OPTARG\" [OPTIND=${OPTIND}]"
	    if [ "$OPTARG" = force ] || [ "$OPTARG" = bootstrap ]
            then
              FLAG=$OPTARG 
            else
	          echo "-m only takes 'force' and 'bootstrap', or leave -m off entirely. Exiting."
              exit
        fi
        ;;
	# Note that options 'p' and 'm' must have an associated argument,
	#+ otherwise it falls through to the default.
	* ) # Default.
	    echo "Unimplemented option chosen. Exiting"
        exit
        ;; 
    esac
  done
shift $(($OPTIND - 1))
# Decrements the argument pointer so it points to next argument.
# $1 now references the first non-option item supplied on the command-line
#+ if one exists.


echo "command line option FLAG -m = $FLAG"
echo "command line option destFLAG -p = $destFlag"

#####################################################################
# INITIALIZING VARIABLES: 
# DEVICEID, SOURCE, DEST, loadDir
#####################################################################
# DEVICEID   If more than one device running on system, must specify explicitly
#            ex: DEVICEID="-s I997cd0fbdbe"
#                DEVICEID="-e"  #for only emulator device
# SOURCE   is location of csv files stored on the Android phone's sdcard
#          FYI, ls -l reveals that sdcard is a symlink to /mnt/sdcard
#  
#     lrwxrwxrwx root     root   2011-11-27 19:15 sdcard -> /mnt/sdcard
#
#
#
# DEST     is the path to the RAPIDANDROID_OUTPUT directory. It can be fully
#          specified, or can use shortcuts of "." and ".." for relative pathname
#
#          ex: DEST="C:\the-path-to-rapidandroid_output\RAPIDANDROID_OUTPUT"
#              DEST = "../RAPIDANDROID_OUTPUT"
# get current folder as $DEST


#DEVICEID="-s emulator-5554"
DEVICEID=""

SOURCE="sdcard/rapidandroid"

#destPwd=$destFlag
destPwd="../.."
DEST="$destPwd/RAPIDANDROID_OUTPUT"
echo "DEST=$DEST"


##########################################################
# PROCESS FILES ON ANDROID DEVICE
# (uses DEVICEID, SOURCE, DEST variables defined above)
##########################################################
echo ""
echo "-------------------------------------"
echo "pulling files from the Android device"
echo "-------------------------------------"
echo ""

#TODO: enhancement- add in logic to exit if adb not setup correctly.
echo "exitcode=" $?

adb $DEVICEID shell "mkdir $SOURCE/pulledexports"
adb $DEVICEID pull "$SOURCE/exports" $DEST
adb $DEVICEID shell "mv $SOURCE/exports/* $SOURCE/pulledexports"
echo "exitcode=" $?

#####################################################
# SETUP CSV OUTPUT FOLDER 
# directory and prefix for processed csv output files
#####################################################

loadDirName=loadingzone
loadDir="../$loadDirName"
LOADFILE="$loadDirName/loadme"
echo "loadDir=$loadDir, LOADFILE=$LOADFILE"

##########################
## MAKE THE DIRECTORIES ##
##########################
if [ -d "$loadDir" ]; then
   true #http://ubuntuforums.org/showthread.php?t=943824 no empty statments
else
  mkdir "$loadDir"
fi


#############################################
## COUNT THE CSV FILES FOR UPPER THRESHOLD ##
## VARIABLES: COUNT, UPPER		   ##
#############################################
echo "leaving -  `pwd`"
cd ..
echo "entering -  `pwd`"

echo ""
echo "-------------------"
echo "Counting .csv files" 
echo "-------------------"
echo ""


# lists all csv, sorts by mod time ascend, counts number of lines in output
COUNT=`ls *.csv -t | wc -l`
echo "Total csv file count is $COUNT"

UPPER=$(($COUNT - 1))
echo "Upper value is $UPPER"


## BEGIN PSEUDO - THE LOGICAL CHECKS OF THE FILES ##
#    
#   This is psuedo code to explain what the remainder of the script does.
#
#   if count (*.csv) > 1
#  	  	#Ready to diff csv files
# 	  	call DIFF()
# 	if count(*.csv) == 1 && flag==force
#		#assumed force load
#		#Single CSV and force, no diffing to occur just force load
#		copy CSV to loadDir
#		delete CSV from dir
# 	if count(*.csv) == 1 && flag==bootsrap
#		#assumed bootstrap
#		#Single CSV and bootsrapping, no diffing to occur just a bootstrap load, CSV will sit for future diff
#		copy CSV to loadDir
#		leave CSV sitting in dir
# 	if count(*.csv) == 1 && flag==null
#		#the CSV is from previous diff-set
#		leave CSV sitting in dir do nothing
# 	if count (*.csv) == 0
#		#No files to diff, exiting
# 		EXIT
#
## END PSEUDO - THE LOGICAL CHECKS OF THE FILES ##

echo ""
echo "-------------------------------------------------------------"
echo "Determining runtime mode based on -m flag and .csv file count"
echo "-------------------------------------------------------------"
echo ""

echo "NOTE: THE FLAG = $FLAG"
if [ -z "$FLAG" ]; then
  echo "-z FLAG true" #z null arg
fi
if [ -n "$FLAG" ]; then
  echo "-n FLAG true" #n !null arg
fi


if expr $COUNT ">" 1 &> /dev/null
  then
    if [ -z "$FLAG" ] &> /dev/null
      then
        echo "count is > 1, FLAG is null, this is normal diff mode for a live system, diff to occur now."
	    func_diff $UPPER
    elif expr $FLAG "=" force &> /dev/null
      then
        echo "count is > 1, FLAG=force, pre-load 1st csv, then diff, then remove the remaining csv file"
	    func_copy1stCsv $LOADFILE
	    func_diff $UPPER
	    rm *.csv
    else expr $FLAG "=" bootstrap &> /dev/null
      echo "count is > 1, FLAG=bootstrap, pre-load 1st csv, then diff,
            leave the remaining csv file for normal diff mode to take over."
	  func_copy1stCsv $LOADFILE
	  func_diff $UPPER
    fi   
     
elif expr $COUNT "=" 1 &> /dev/null
  then
    if [ -z "$FLAG" ] &> /dev/null
	  then 
	    echo "count == 1, FLAG is null, this is normal diff mode for a live system, wait for next file.
			 The CSV is from a diff-set, will leave CSV here for next run."
	elif expr $FLAG "=" force &> /dev/null
	  then
	    echo "count == 1, FLAG=force, No diffing to occur will force load and remove the csv file."
		func_move1stCsv $LOADFILE
	else expr $FLAG "=" bootstrap &> /dev/null
	  echo "count == 1, FLAG=bootstrap, No diffing to occur just a bootstrap load, 
	        the CSV will sit for future diff."
	  func_copy1stCsv $LOADFILE
	fi
	
else expr $COUNT "=" 0 &> /dev/null
    echo "count is 0, No files to diff, exiting"
    exit 1 
fi

################ END pullCsvAndDiff.sh ###################################
echo "leaving -  `pwd`"
cd scripts
echo "entering -  `pwd`"

DONE="---------------Completed pull csv and diff--------------------"
echo ""
echo ""
echo ""
echo "-------------------------------------------------------------"
echo $DONE
echo "-------------------------------------------------------------"
