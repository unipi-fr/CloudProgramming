import random

if __name__ == "__main__":

    print('Number of points:')
    n_points = int(input())

    print('Dimentions:')
    dimentions = int(input())

    file = open("points.txt", "a")

    for i in range(0, n_points):

        point = ""

        for j in range(0, dimentions - 1):
            point += str(random.random()) + ","
        point += str(random.random())
        
        file.write(point + "\n")

    file.close()