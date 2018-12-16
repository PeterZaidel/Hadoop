#!/usr/bin/env bash
hdfs dfs -rm -r /user/p.zaydel/sparkPR/sparkPrResult/

spark-submit --master yarn --deploy-mode cluster \
    --num-executors 5 --executor-cores 2 --executor-memory 3G \
    SparkPageRank.py  /user/v.belyaev/soc-LiveJournal1.txt  /user/p.zaydel/sparkPR/sparkPrResult/