#!/usr/bin/env bash
inDir="/data/hw4/soc-LiveJournal1.txt.gz"
outDir="/user/p.zaydel/sparkPR/"

classCount="CountNodesJob"
classInitJob="InitDataJob"
classPageRankJob="PageRankJob"
classSortingJob="SortingJob"

initOut=$outDir"init_out/"

pageRankInDir=$initOut"part-*"
pageRankOutDir=$outDir"pr_out/"
hdfs dfs -rm -r $pageRankOutDir
wait

hadoop jar mapred.jar  $classPageRankJob  $pageRankInDir  $pageRankOutDir
wait

sortingInDir=$pageRankOutDir"part-*"
sortingOutDir=$outDir"sorted_pr/"

hadoop jar mapred.jar  $classSortingJob  $sortingInDir  $sortingOutDir
wait