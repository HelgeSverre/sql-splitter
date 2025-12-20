#!/bin/bash

####
# Split MySQL dump SQL file into one file per table
# based on http://blog.tty.nl/2011/12/28/splitting-a-database-dump
####

if [ $# -lt 1 ] ; then
  echo "USAGE $0 DUMP_FILE [TABLE]"
  exit
fi

if [ $# -ge 2 ] ; then
  csplit -s -ftable $1 "/-- Table structure for table/" "%-- Table structure for table \`$2\`%" "/-- Table structure for table/" "%40103 SET TIME_ZONE=@OLD_TIME_ZONE%1"
else
  csplit -s -ftable $1 "/-- Table structure for table/" {*}
fi

[ $? -eq 0 ] || exit

mv table00 head

FILE=`ls -1 table* | tail -n 1`
if [ $# -ge 2 ] ; then
  mv $FILE foot
else
  csplit -b '%d' -s -f$FILE $FILE "/40103 SET TIME_ZONE=@OLD_TIME_ZONE/" {*}
  mv ${FILE}1 foot
fi

for FILE in `ls -1 table*`; do
  NAME=`head -n1 $FILE | cut -d$'\x60' -f2`
  cat head $FILE foot > "$NAME.sql"
done

rm head foot table*