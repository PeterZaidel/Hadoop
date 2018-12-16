import sys
from operator import add, itemgetter
from pyspark import SparkContext, SparkConf

alpha = 0.1
iterations = 10


def compute_accumulated_pr(x):
    node, (nodes_out, rank) = x
    if nodes_out is not None and len(nodes_out) == 0:
        return [rank]

    if nodes_out is None:
        return [rank]

    return [0.0]


def init_mapper(p, init_rank):

    return (p[0], init_rank)


def pr_mapper(link, rank, out_list):
    if len(out_list) == 0:
        yield (link, 0)

    if len(out_list) > 0:
        yield (link, 0)
        out_rank = rank/float(len(out_list))
        for n in out_list:
            yield (n, out_rank)


def pr_reducer(link, iter_rank, N, accumultedPr):
    sum_rank = 0

    if iter_rank is not None:
        for r in iter_rank:
            sum_rank += r

    rank = alpha*1.0/float(N) + (1.0 - alpha) * (sum_rank + accumultedPr/float(N))
    return (link, rank)




def main(input_file, output_dir):

    print("--------------------------------------")
    print("------START PAGE RANK SPARK-----------")
    print("--------------------------------------")
    SparkContext.setSystemProperty('spark.executor.memory', '3g')
    conf = SparkConf().setAppName("SparkPageRank")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_file)

    uniqueEdges = lines.filter(lambda x: len(x) > 0 and x[0] != '#')\
        .flatMap(lambda x: map(int, x.strip().split())).distinct().sortBy(lambda p:p, True).map(lambda x: [x, None])

    N = 4847571

    prob = 1.0 / float(N)


    edges = lines.filter(lambda x: len(x) > 0 and x[0] != '#') \
                 .map(lambda x: map(int, x.strip().split()))   \
                 .groupByKey()

    def _edges_list_prep(x):
        values = x[1]
        if values[0] is None and values[1] is None:
            return (x[0], [])
        if values[0] is not None and values[1] is None:
            return (x[0], values[0])
        if values[0] is None and values[1] is not None:
            return (x[0], values[1])
        if values[0] is not None and values[1] is not None:
            return (x[0], values[0] + values[1])


    print("allEdges cahching....")

    allEdges = uniqueEdges.leftOuterJoin(edges).map(_edges_list_prep)

    print("ranks cahching....")
    ranks = allEdges.map(lambda p: init_mapper(p, prob)).cache() #(p[0], prob))

    print("sumRank calculation....")
    sumRank = ranks.map(lambda p: p[1]).sum()

    print("accumulatedPR calculation....")
    accumPr = allEdges.fullOuterJoin(ranks).flatMap(compute_accumulated_pr).sum()

    print("ITER INIT SUM PR:", sumRank)
    print("ITER INIT ACCUM PR: ", accumPr)


    for i in range(iterations):
        print("ITER {0}".format(i))

        emits = allEdges.fullOuterJoin(ranks).flatMap(lambda p: pr_mapper(p[0], p[1][1], p[1][0]))
        print("--ranks cahching....")
        ranks = emits.groupByKey().map(lambda p: pr_reducer(p[0], p[1], N, accumPr)).cache()

        print("--sumRank calculation....")
        sumRank = ranks.map(lambda p: p[1]).sum()
        print("--accumulatedPR calculation....")
        accumPr = allEdges.fullOuterJoin(ranks).flatMap(compute_accumulated_pr).sum()

        print("--SUM PR:", sumRank)
        print("--ACCUM PR: ", accumPr)

    ranks = ranks.sortBy(itemgetter(1), ascending=False)
    ranks.saveAsTextFile(output_dir + "ranks/")


import shutil
if __name__ == "__main__":
    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    try:
        shutil.rmtree(output_dir, True)
    except:
        pass

    main(input_file, output_dir)
