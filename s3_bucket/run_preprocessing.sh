while getopts i option
do case "${option}" in
i)	echo "Usage:"
	echo "		-info
	This script extracts the month names of the file to be preprocessed according to the 'config.txt' file.
	Each of the preprocessed files are renamed with a 'proc_' prefix.
	All processed files are then merged into a single file 'RS_data.csv'.
	Lastly, based on the first and the last timestamp, the file 'days_scheduler.txt' is created.
	This file will eventually interact with an AWS scheduler which, by extracting the rows sequentially, will train the model
	only using the rows of the dataset that refers to a specific day."
	exit 0 
	;;
esac
done

#---------------------------------------

while read m;
do
	echo '-----------------------------------'
	echo $m.csv
    python3 -W ignore preprocessing.py $m.csv
done < config.txt

awk 'FNR==1 && NR!=1{next;}{print}' proc_*.csv  >> RS_data.csv

python3 -W ignore create_scheduler.py 