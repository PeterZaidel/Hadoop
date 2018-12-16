#!/usr/bin/env bash
inDir="/data/hw4/soc-LiveJournal1.txt.gz"
outDir="/user/p.zaydel/sparkPR/"

hdfs dfs -rm -r $outDir
wait

classCount="CountNodesJob"
classInitJob="InitDataJob"
classPageRankJob="PageRankJob"
classSortingJob="SortingJob"

initIn=$inDir
initOut=$outDir"init_out/"
hadoop jar mapred.jar  $classInitJob  $initIn  $initOut
wait

pageRankInDir=$initOut"part-*"
pageRankOutDir=$outDir"pr_out/"

hadoop jar mapred.jar  $classPageRankJob  $pageRankInDir  $pageRankOutDir
wait

sortingInDir=$pageRankOutDir"part-*"
sortingOutDir=$outDir"sorted_pr/"

hadoop jar mapred.jar  $classSortingJob  $sortingInDir  $sortingOutDir
wait