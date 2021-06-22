while getopts h option
do case "${option}" in
h)	echo "Info:"
	echo "This script will iterate though each day (raw in scheduler.txt) and extract the rows 
	corresponding to that day to train the model. At the end of the training the script will sleep 
	for 10 minutes and then train the data corresponding to the next day."
    exit 0 
	;;
esac
done

#---------------------------------------

while read day
do
	batch=$(grep $day RS_data.csv)
	python3 train.py $batch
	sleep 10m
done < days_scheduler.txt