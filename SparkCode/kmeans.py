import sys
import numpy as np
import random
from operator import add

from pyspark import SparkContext

def create_point(line):
    lineSplit = line.split(",")
    point = np.array(lineSplit).astype(np.float)
    return point

def map_function(point):
    minDis = float("inf")
    pointArr = np.array(point)
    groupCentroid = []
    index = 0
    for centroid in centroidsVar.value:
        centroidArr = np.array(centroid)
        dis = np.linalg.norm(pointArr - centroidArr)
        if dis < minDis:
            minDis = dis
            groupCentroid = index
        index += 1
    return (groupCentroid, point)

def createCombiner(point):
    return (point, 1)
    
def mergeValue(partial_sum, point):
    return (partial_sum[0] + point, partial_sum[1] + 1)
    
def mergeCombiner(partial_sum1, partial_sum2):
    return (partial_sum1[0] + partial_sum2[0], partial_sum1[1] + partial_sum2[1])


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

    print(lines.take(100))

    # points = lines.map(lambda x: x.split(",").astype(numpy.float))
    points = lines.map(create_point)

    print(points.take(100))

    withReplacement = False                                             #Il campo withReplacement = False, specifica che non sono ammessi duplicati
    randomSeedValue = random.randrange(sys.maxsize)
    print("INFO | seed to select centroids = {randomSeedValue}")
    centroids = points.takeSample(withReplacement, numberOfCentroids, randomSeedValue)
    centroidsVar = sc.broadcast(centroids)                              #Condivido in READ-ONLY i centroid per tutti i task spark

    mapped_points = points.map(map_function)
    combined_points = mapped_points.combineByKey(createCombiner, mergeValue, mergeCombiner)
    new_centroids = combined_points.map(lambda x:(x[0],x[1][0]/x[1][1]))
    
    #words = lines.flatMap(lambda x: x.split(' '))
    #ones =  words.map(lambda x: (x, 1))
    #counts = ones.reduceByKey(add)

    #if len(sys.argv) == 3:
    #    counts.repartition(1).saveAsTextFile(sys.argv[2])
    #else:
    #    output = counts.collect()
    #    for (word, count) in output:
    #        print("%s: %i" % (word, count))
