iterations=$1
day=$2

while getopts i:d:h option
do case "${option}" in
d) day="${OPTARG}" ;;
i) iterations="${OPTARG}" ;;
h)	echo "Usage:"
	echo "		-h	Display this help message."
    echo "		-i 	Define the number of iterations for the RS."		  
	echo "		-d	Date passed by the scheduler to parse dataset."
    exit 0 
	;;
esac
done

#---------------------------------------

batch=$(grep $2 RS_data.csv)
python3 train.py $batch 1000

