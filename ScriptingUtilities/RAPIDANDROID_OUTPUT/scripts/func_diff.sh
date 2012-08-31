#!/bin/bash
#########################################
# AUTHOR pokuam1  		        #
# ORIGINAL DATE 1/08/11                 #
# REFACTOR DATE 12/06/11	        #
#########################################

func_diff()
{
#diff 2 files until only 1 file remains

echo ""
echo ""
echo "-------------------------------------------------------------"
echo "Starting diff function. Diffing .csv files for any changes" 
echo "-------------------------------------------------------------"
echo ""

locUPPER=$1
CSVEXT=.csv

#for i in $(seq 1 1 $locUPPER)
a=1
while [ "$a" -le $locUPPER ]
 do
   
    # always compare the oldest files at iteration 
	FILEA=`ls -t *.csv | tail -1`
	echo FileA is "$FILEA"
	FILEB=`ls -t *.csv | tail -2 | head -1`
	echo FileB is "$FILEB"
	
	TS=`date +%d%m%y`-`date +%H%M%S`
	# put the column header in each loadfile
	head -1 "$FILEA" > "$LOADFILE"_$a-$TS"$CSVEXT"
	
	# for sanity output to the console (testing)
	#diff -e $FILEA $FILEB  | sed -e '1d' | sed -e '$d'
	
	# for sanity append the diff to the difflog
	diff -e "$FILEA" "$FILEB"  | sed -e '1d' | sed -e '$d' >> difflog.txt
	
	# actual output to file that will be ingested by ETL script. remove the oldest file of each comparison.
	diff -e "$FILEA" "$FILEB"  | sed -e '1d' | sed -e '$d' >> "$LOADFILE"_$a-$TS"$CSVEXT" && rm "$FILEA"
	#continue
	echo "$a "
	let "a+=1"
 done

}


func_copy1stCsv()
{
echo "-------------------------------------------------------------------"
echo "copying the 1st csv to be loaded ('-m force|bootstrap' was invoked)" 
echo "-------------------------------------------------------------------"


LOADFILE=$1
j=0
CSVEXT=.csv

CSV1=`ls -t *.csv | tail -1`

TS=`date +%d%m%y`-`date +%H%M%S`
CSV1COPY="$LOADFILE"_$j-$TS"$CSVEXT"
cp $CSV1 $CSV1COPY
echo "The 1st csv, $CSV1, was copied to $CSV1COPY"

}


func_move1stCsv()
{
echo "------------------------------------------------------------------"
echo "moving the 1st csv to be loaded ('-m force|bootstrap' was invoked)" 
echo "------------------------------------------------------------------"

LOADFILE=$1
j=0
CSVEXT=.csv

CSV1=`ls -t *.csv | tail -1`


TS=`date +%d%m%y`-`date +%H%M%S`
CSV1MOVE="$LOADFILE"_$j-$TS"$CSVEXT"
mv $CSV1 $CSV1MOVE
echo "The 1st csv, $CSV1, was moved to $CSV1MOVE"

}
