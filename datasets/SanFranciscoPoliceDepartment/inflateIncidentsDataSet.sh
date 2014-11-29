#! /bin/bash

INCIDENT_FILE=SFPD_Incidents_Previous_Three_Months.csv
INFLATED_EVENTS_FILE=SFPD_Incidents_Previous_Three_Months_Inflated.csv

cp $INCIDENT_FILE $INFLATED_EVENTS_FILE

FACTOR=20
if [ "$#" == "1" ]; then
  FACTOR=$1
fi


echo Inflating $INCIDENT_FILE of a factor of $FACTOR into file $INFLATED_EVENTS_FILE

for i in $(seq 2 1 ${FACTOR})
do
  tail -n +2 $INCIDENT_FILE >> $INFLATED_EVENTS_FILE
  echo -n .
done


