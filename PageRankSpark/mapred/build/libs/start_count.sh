#!/usr/bin/env bash

inDir="/data/hw4/soc-LiveJournal1.txt.gz"
outDir="/user/p.zaydel/sparkPR/count-out/"
className="CountNodesJob"

hdfs dfs -rm -r $outDir
hadoop jar mapred.jar  $className  $inDir  $outDir