while getopts i option
do case "${option}" in
i)	echo "Usage:"
	echo "-info
	This script extracts the month names of the file to be preprocessed according to the 'config.txt' file.
	Each of the preprocessed files are renamed with a 'proc_' prefix.
	All processed files are then merged into a single file 'RS_data.csv'.
	Lastly, based on the first and the last timestamp, the file 'days_scheduler.txt' is created."
	exit 0 
	;;
esac
done

#---------------------------------------

while read month;
do
	echo '-----------------------------------'
	echo $month.csv
    python3 -W ignore preprocessing.py $month.csv
done < config.txt

awk 'FNR==1 && NR!=1{next;}{print}' proc_*.csv  >> RS_data.csv

python3 -W ignore create_scheduler.py 