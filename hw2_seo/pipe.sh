#!/usr/bin/env bash
DEBUG=false
PARSE=true
if ${PARSE}; then
echo "Parsing"
hdfs dfs -rm -r hw_seo
hdfs dfs -mkdir hw_seo
if ${DEBUG}; then
echo "DEBUG"
hadoop jar hw2_seo.jar GetClicksJob /data/hw2/clicks/2016.08.06/part-01.gz hw_seo/parsedLinks
else
echo "RELEASE"
hadoop jar hw2_seo.jar GetClicksJob /data/hw2/clicks/*/*.gz hw_seo/parsedLinks
fi
fi
hdfs dfs -rm -r hw_seo/result0
hdfs dfs -rm -r hw_seo/result100
hadoop jar hw2_seo.jar SecondarySort hw_seo/parsedLinks/part* hw_seo/result0
hadoop jar hw2_seo.jar SecondarySort hw_seo/parsedLinks/part* hw_seo/result100 100