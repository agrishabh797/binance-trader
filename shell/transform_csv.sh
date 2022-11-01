#!/bin/bash

# This file copies the given source files to a archive directory and transforms the csv to desired format
# This script also concats the files into a single file for copying it to the db

get_expiry_date() {
    symbol=$1
    futures=${symbol:0-3}
    options=${symbol:0-2}
    #echo $futures
    #echo $options
    if [ $futures == "FUT" ];
    then
        date_part=`echo $symbol | grep -oP '[0-9]{2}[A-Z]{3}'`
        year_part=`echo $date_part | grep -oP '[0-9]{2}'`
        month_part=`echo $date_part | grep -oP '[A-Z]{3}'`
        expiry_date=`date -d "01-$month_part-20$year_part" +%Y-%m-%d`
    elif [ $options == "CE" -o $options == "PE" ];
    then
        date_part=`echo $symbol | grep -oP '[0-9]{6}'`
        expiry_date=`date -d "20$date_part" +%Y-%m-%d`
    else
        echo "Bad Product"
        exit 1
    fi
    echo $expiry_date
}


target_dir=$1
target_final_file_name=$target_dir/$2

i=0

for var in "$@"
do
    # first two variables are target directory and target_file_name, ignoring those and reading the list of source of files
    if [ $i -gt 1 ]
    then
        cp $var $target_dir
        file_name=`basename $var`
        file_name_full=$target_dir/$file_name
        #echo $file_name
        #echo $file_name_full
        dos2unix -q $file_name_full
        suffix='.csv'
        symbol=${file_name%"$suffix"}

        expiry_date=$(get_expiry_date $symbol)
        #echo $expiry_date
        sed -i -e 's/20[0-9]\{2\}/&-/' $file_name_full
        sed -i -e 's/[0-9]\{4\}-[0-9][0-9]/&-/' $file_name_full
        sed -i -e s/,/' '/ $file_name_full
        sed -i -e "s/^/${symbol},/" $file_name_full
        sed -i -e "s/$/,${expiry_date}/" $file_name_full
        cat $file_name_full >> $target_final_file_name
        rm $file_name_full
    fi
    i=`expr $i + 1`
done
