#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='target/scala-2.10/rdd_filter_2.10-0.1.jar'
Call_file_suffix='-ModalCallVolDegree.csv'


exec_obj_name='RDD_Filter'


#month_array=(0604 ) 

for month in 0603
do
	
	 
	monthn1=$month
	echo ' Monthl1 is ' $monthl1 ' and monthl2 is ' $monthl2
	spark-submit --class $exec_obj_name --master yarn --driver-memory 28G \
        --executor-memory 44G \
        --executor-cores 28 \
        --num-executors 3 \
	$jar $monthn1  $monthn1$Call_file_suffix   --verbose ;
done
