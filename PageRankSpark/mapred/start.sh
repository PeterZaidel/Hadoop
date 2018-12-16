#!/usr/bin/env bash
#hdfs dfs -rm -r /user/p.zaydel/ir-hw3/uniq_links/
#hadoop jar build/libs/hw3.jar UniqLinksJob   /data/infopoisk/hits_pagerank/docs-*    /user/p.zaydel/ir-hw3/uniq_links   /data/infopoisk/hits_pagerank/urls.txt

#hdfs dfs -rm -r /user/p.zaydel/ir-hw3/graph/
#hadoop jar build/libs/hw3.jar LinkGraphJob   /data/infopoisk/hits_pagerank/docs-000.txt    /user/p.zaydel/ir-hw3/graph  /user/p.zaydel/ir-hw3/uniq_links/part-*  /data/infopoisk/hits_pagerank/urls.txt
#hadoop jar build/libs/hw3.jar LinkGraphJob   /data/infopoisk/hits_pagerank/docs-*.txt    /user/p.zaydel/ir-hw3/graph

#hdfs dfs -rm -r /user/p.zaydel/ir-hw3/page-rank/
#hadoop jar hw3.jar PageRankJob   /user/p.zaydel/ir-hw3/graph/part-*    /user/p.zaydel/ir-hw3/page-rank
##
#hdfs dfs -rm -r /user/p.zaydel/ir-hw3/hits/
#hadoop jar build/libs/hw3.jar HITSJob   /user/p.zaydel/ir-hw3/graph/part-*    /user/p.zaydel/ir-hw3/hits

hdfs dfs -rm -r /user/p.zaydel/ir-hw3/sorted/
hadoop jar hw3.jar  SortingJob  /user/p.zaydel/ir-hw3/page-rank/it03/part-*  /user/p.zaydel/ir-hw3/hits/it03/part-*  /user/p.zaydel/ir-hw3/sorted/

hdfs dfs -rm -r /user/p.zaydel/ir-hw3/sorted-2/
hadoop jar hw3.jar  SortingJob  /user/p.zaydel/ir-hw3/page-rank/it03/part-*  /user/p.zaydel/ir-hw3/hits/it03/part-*  /user/p.zaydel/ir-hw3/sorted-2/




