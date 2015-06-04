#/bin/sh

for f in `ls *.csv`
do
echo 'Processing  '$f
	python cleaner.py $f
done
