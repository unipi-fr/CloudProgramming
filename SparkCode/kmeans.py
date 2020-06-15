import sys
import numpy
import random
from operator import add

from pyspark import SparkContext

def create_point(line):
    point = line.split(",").astype(numpy.float)
    
    return point

def map_function(point):
    minDis = float("inf")

    pointArr = numpy.array(point);

    groupCentroid = [];

    for centroid in centroidsVar.value:
        centroidArr = numpy.array(centroid);
        dis = numpy.linalg.norm(pointArr - centroidArr);
        if dis < minDis:
            minDis = dis;
            groupCentroid = centroid;

    return (groupCentroid, point);

def reduce_function(centroid, point):
    sum = 0;

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: KMeans <input file (points)> <dimensions> <centroids> <Stop Criteria> <max iteration> [<output file>]", file=sys.stderr)
        sys.exit(-1)
    inputFilePath = sys.argv[1]
    pointsDimension = sys.argv[2]
    numberOfCentroids = sys.argv[3]
    stopCriteria = sys.argv[4]
    maxIterations = sys.argv[5]
    outputPath = sys.argv[6]

        # Print degli argomenti da riga di comando
    print("INFO | args[0]: <input points> = {inputFilePath}")           #File di input
    print("INFO | args[1]: <d> = {pointsDimension}")                    # Numero componenti per punto
    print("INFO | args[2]: <k> = {numberOfCentroids}")                  # Numero centroidi => numero cluster
    print("INFO | args[3]: Stop Criteria = {stopCriteria}")             
    print("INFO | args[4]: <max_iterations> = {maxIterations}")             
    print("INFO | args[5]: <output file (centroids)> = {outputPath}")   # Cartella di output

    master = "yarn"
    sc = SparkContext(master, "KMeans")
    

    lines = sc.textFile(inputFilePath)
    # points = lines.map(lambda x: x.split(",").astype(numpy.float))
    points = lines.flatMap(create_point)
    withReplacement = False                                             #Il campo withReplacement = False, specifica che non sono ammessi duplicati
    randomSeedValue = random.randrange(sys.maxsize)
    print("INFO | seed to select centroids = {randomSeedValue}")
    centroids = points.takeSample(withReplacement, numberOfCentroids, randomSeedValue)
    centroidsVar = sc.broadcast(centroids)                              #Condivido in READ-ONLY i centroid per tutti i task spark

    
    #words = lines.flatMap(lambda x: x.split(' '))
    #ones =  words.map(lambda x: (x, 1))
    #counts = ones.reduceByKey(add)

    #if len(sys.argv) == 3:
    #    counts.repartition(1).saveAsTextFile(sys.argv[2])
    #else:
    #    output = counts.collect()
    #    for (word, count) in output:
    #        print("%s: %i" % (word, count))