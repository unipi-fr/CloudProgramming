import sys
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: KMeans <input file (points)> <dimensions> <centroids> <Stop Criteria> <max iteration> [<output file>]", file=sys.stderr)
        sys.exit(-1)
    inputFilePath = sys.argv[1]
    inputFilePath = sys.argv[1]

        # Print degli argomenti da riga di comando
    print("INFO | args[0]: <input points> = ", file=sys.stdout)   #File di input
    print("INFO | args[1]: <d> = ", file=sys.stdout)              # Numero componenti per punto
    print("INFO | args[2]: <k> = ", file=sys.stdout)              # Numero centroidi = numero cluster
    print("INFO | args[3]: Stop Criteria = ", file=sys.stdout)             
    print("INFO | args[4]: <max_iterations> = ", file=sys.stdout)             
    print("INFO | args[5]: <output file (centroids)> = ", file=sys.stdout)   # Cartella di output

    master = "yarn"
    sc = SparkContext(master, "KMeans")

    #lines = sc.textFile(sys.argv[1])
    #words = lines.flatMap(lambda x: x.split(' '))
    #ones =  words.map(lambda x: (x, 1))
    #counts = ones.reduceByKey(add)

    #if len(sys.argv) == 3:
    #    counts.repartition(1).saveAsTextFile(sys.argv[2])
    #else:
    #    output = counts.collect()
    #    for (word, count) in output:
    #        print("%s: %i" % (word, count))