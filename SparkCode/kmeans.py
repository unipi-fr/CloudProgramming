import sys
import random
import numpy as np
import time
from pyspark import SparkContext

def createPoint(line):
    # Prende in ingresso una linea del file di input e la trasforma in un numpy array di float
    lineSplit = line.split(",")
    point = np.array(lineSplit).astype(np.float)
    return point

def mapFunction(line):
    # Prende in ingresso un punto e calcola la distanza tra esso e i vari centroidi, trovando quello più vicino
    point = createPoint(line)
    minDistance = float("inf")
    nearestCentroidIndex = 0
    index = 0
    # broadcastCentroids contiene la lista dei centroidi al passo precedente
    for centroid in broadcastCentroids.value:
        distance = np.linalg.norm(point - centroid)
        if distance < minDistance:
            minDistance = distance
            nearestCentroidIndex = index
        index += 1
    # Restituisce l'indice del centroide più vicino e il punto associato
    return (nearestCentroidIndex, point)

def createCombiner(point):
    # Affianca un uno ad ogni punto per calcolare la somma
    return (point, 1)
    
def mergeValue(partialSum, point): 
    # Somma i punti all'accumulatore
    return (partialSum[0] + point[0], partialSum[1] + 1)
    
def mergeCombiner(partialSum1, partialSum2): 
    # Esegue il merge delle somme parziali 
    return (partialSum1[0] + partialSum2[0], partialSum1[1] + partialSum2[1])

def totalDistanceOldNewCentroid(oldCentroids, newCentroids):
    # Somma le distanze tra i punti delle due liste
    totalDistance = 0.0
    for i in range(0,len(oldCentroids)):
        distance = np.linalg.norm(oldCentroids[i] - newCentroids[i])
        totalDistance += distance
    return totalDistance

if __name__ == "__main__":

    # Tempo a inizio esecuzione
    startTime = time.time()
    oldTime = startTime

    # Assegnazione argomenti
    if len(sys.argv) != 7:
        print("Usage: KMeans <input file (points)> <dimensions> <centroids> <Stop Criteria> <max iteration> [<output file>]", file=sys.stderr)
        sys.exit(-1)

    inputFilePath = sys.argv[1]
    pointsDimensions = int(sys.argv[2])
    numberOfCentroids = int(sys.argv[3])
    # Margine di differenza tra i centroidi di due iterazioni consecutive sotto il quale stoppare l'algoritmo
    stopCriteriaMargin = float(sys.argv[4])
    maxIterations = int(sys.argv[5])
    outputPath = sys.argv[6]

    # Stampa degli argomenti su riga di comando
    print("INFO | args[0]: <input points> = "+ str(inputFilePath))           # File di input
    print("INFO | args[1]: <d> = "+ str(pointsDimensions))                   # Numero componenti per punto
    print("INFO | args[2]: <k> = "+ str(numberOfCentroids))                  # Numero centroidi = numero cluster
    print("INFO | args[3]: Stop Criteria = "+ str(stopCriteriaMargin))       # Margine per il criterio di stop    
    print("INFO | args[4]: <max_iterations> = "+ str(maxIterations))         # Numero massimo di iterazioni consentite
    print("INFO | args[5]: <output file (centroids)> = "+ str(outputPath))   # Cartella di output

    master = "yarn"
    sc = SparkContext(master, "KMeans")

    # Sopprime i log
    sc.setLogLevel("ERROR")

    # Caricamento del file di input nel contesto
    inputLines = sc.textFile(inputFilePath)

    # Campionamento casueale dei centroidi, senza rimpiazzo
    centroidsLines = inputLines.takeSample(withReplacement = False, num = numberOfCentroids, seed = random.randrange(sys.maxsize))

    # Conversione dei centroidi in punti 
    oldCentroids = [createPoint(line) for line in centroidsLines]

    # Broadcast in READ-ONLY dei centroid per tutti i task spark
    broadcastCentroids = sc.broadcast(oldCentroids) 
    # Margine di movimento dei centroidi dal passo precedente
    centroidsMovementMargin = sys.maxsize

    # Iterazioni dell'algoritmo
    for iteration in range(0, maxIterations):

        print("INFO | Starts of iteration " + str(iteration) + " ...")

        # Assegnazione punti al centroide più vicino
        mappedPoints = inputLines.map(mapFunction)

        # Calcolo delle somme parziali e poi totali dei punti assegnati ai centroidi
        combinedPoints = mappedPoints.combineByKey(createCombiner, mergeValue, mergeCombiner)

        # I componenti delle somme totali (x[1][0]) vengono divisi per il numero di punti sommati (x[1][1])
        # "new_centroid" i nuovi valori delle componenti dei centroidi e i relativi indici (x[0])
        indexesAndCentroids = combinedPoints.map(lambda x:(x[0],x[1][0]/x[1][1]))
        
        # Riordina i centroidi per effettuare il confronto
        newCentroids = []
        for indexAndCentroid in indexesAndCentroids.collect():
            newCentroids.insert(indexAndCentroid[0],indexAndCentroid[1])

        print("INFO | Old centroids: " + str(oldCentroids))
        print("INFO | New centroids: " + str(newCentroids))

        # Calcolo del margine di spostamento da un'iterazione all'altra
        centroidsMovementMargin = totalDistanceOldNewCentroid(oldCentroids, newCentroids)
        print("INFO | Movement margin = " + str(centroidsMovementMargin))

        newTime = time.time()
        print("INFO | End iteration " + str(iteration) + " in " + str(newTime - oldTime) + " seconds from previous one")
        print("INFO | End iteration " + str(iteration) + " at " + str(newTime - startTime) + " seconds from start")
        oldTime = newTime
        
        # Uscita dal ciclo in caso del superamento del margine
        if centroidsMovementMargin <= stopCriteriaMargin:
            break
 
        # Broadcast dei nuovi centroidi
        broadcastCentroids = sc.broadcast(newCentroids)
        oldCentroids = newCentroids

        iteration+=1
    
    print("INFO | Stop iterations at " + str(time.time() - startTime) + " seconds from start")
    print("INFO | Results: " + str(newCentroids))
    indexesAndCentroids.saveAsTextFile(outputPath)
