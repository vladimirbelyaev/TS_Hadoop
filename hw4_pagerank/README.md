### Команда для запуска Spark-задачи:

/bin/spark-submit --class SparkPageRank16 --master yarn --deploy-mode cluster  --driver-memory 2G --num-executors 12 --executor-memory 2G  hw4_pagerank.jar /data/hw4/soc-LiveJournal1.txt.gz hw4_prspark

### Команда для запуска MapReduce-задачи:

sh mapredpipe.sh

##### hint - для более быстрой работы можно извлечь архив и залить *.txt в hdfs

