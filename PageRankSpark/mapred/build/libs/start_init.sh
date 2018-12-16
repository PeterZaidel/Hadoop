#!/usr/bin/env bash
inDir="/data/hw4/soc-LiveJournal1.txt.gz"
outDir="/user/p.zaydel/sparkPR/"



classCount="CountNodesJob"
classInitJob="InitDataJob"
classPageRankJob="PageRankJob"
classSortingJob="SortingJob"

initIn=$inDir
initOut=$outDir"init_out/"

hdfs dfs -rm -r $initOut
wait

hadoop jar mapred.jar  $classInitJob  $initIn  $initOut
wait