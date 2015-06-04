#!/bin/bash
export _JAVA_OPTIONS=-Xmx60G
jar='/export/home/mraza/Rwanda_Agg_Code/target/scala-2.10/cogstats_2.10-0.1.jar'
COG_file_suffix='_MonthlyCOG'
Call_file_suffix='-Call.pai.sordate.txt'
hadoop_outdir_prefix='/user/mraza/Rwanda_Out/'
hadoop_outdir_suffix='*/part-*'

output_path='/data/nas/mraza/Migration_4Months'
output_suffix='_MonthlyCOG'

input_path='hdfs:///user/mraza/Rwanda_Out/9_22/DistrictDegreeMigration/'

#for month in 0501 0502 0503 0504 0505 0506 0507 0508 0509 0510 0511 0512 0601 0602 0603 0604 0605 0606 0607 0608 0609 0610
#for month in 0606
for month in  0604 0803
do
#   	echo "Trying jar $jar file $month$file_suffix ";

hadoop fs -cat Rwanda_Out/CallsVolDegree/filtered${month}-ModalCallVolDegree.csv/part*>> ./${month}-winsorizedMigrationDistrictDegree.csv
#hadoop fs -cat ${input_path}MigrationDistrictDegree${month}/part-00000 >>${output_path}/${month}MigrationDistrictDegree.csv
#hadoop fs -cat ${input_path}MigrationDistrictVolume${month}/part-00000 >>${output_path}/${month}MigrationDistrictVolume.csv
#hadoop fs -cat /user/mraza/Rwanda_Out_7_27/${month}AggResult/part*>>../Rwanda_Output/0727/${month}AggResult
done
