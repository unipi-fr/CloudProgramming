import random
from sklearn.datasets import make_blobs

if __name__ == "__main__":

    centers = []

    print('Dimentions:')
    dimentions = int(input())

    print('Number of centers:')
    n_centers = int(input())

    print('Number of samples per center:')
    n_samples = int(input())

    print('Standard deviation:')
    cluster_std = float(input())

    print('Min center coordinate:')
    min = float(input())

    print('Max center coordinate:')
    max = float(input())

    for i in range(0, n_centers):
    	point = []
    	for j in range(0, dimentions):
            point.append(random.uniform(min, max) )
    	centers.append(point)
    	print("Cluster center " + str(i) + " = " + str(point))

    X, y = make_blobs(n_samples=n_samples, centers=centers, cluster_std=cluster_std, random_state=0)

    file = open("points.txt", "a")

    for point in X:
        line = ""
        for component in point[:-1]:
            line += str(component) + ","
        line += str(point[-1])
        
        file.write(line + "\n")

    file.close()
