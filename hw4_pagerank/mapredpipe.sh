#!/usr/bin/env bash
hdfs dfs -rm -r hw_4pr_mapred/
hdfs dfs -mkdir hw_4pr_mapred/
echo "Initialising PageRank iter_0"
hadoop jar pagerank_hits.jar InitPageRankJob soc-LiveJournal1.txt hw_4pr_mapred/iter_0
echo "Executing PageRank loop"
hadoop jar pagerank_hits.jar PageRankJob hw_4pr_mapred/iter_ 7
echo "Sorting"
hadoop jar pagerank_hits.jar SortJob hw_4pr_mapred/iter_fin hw_4pr_mapred/result
