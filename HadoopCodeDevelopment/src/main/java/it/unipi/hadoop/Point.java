package it.unipi.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;

// Classe che rappresenta un qualsiasi punto
public class Point implements Serializable {

    // Componenti del punto
    public ArrayList<Double> components = new ArrayList<>();
    // Per le somme parziali, numero di punti sommati
    public int summedPoints = 1;
    //-1 se è un punto qualsiasi, altro se indice di un centroide
    public int index = -1; 

    public Point() {}

    // Restituisce la distanza di tipologia distanceType tra due punti, STATICA
    public static double distance(int distanceType, Point P1, Point P2) {
        double sum = 0;
        int index = 0;
        for (double componentP1 : P1.components) {
            double componentP2 = P2.components.get(index);
            sum += Math.pow((componentP1 - componentP2), distanceType);
            index++;
        }
        return Math.pow(sum, 1.0 / distanceType);
    }

    // Somma a questo punto le componenti di un altro punto
    public void sum(Point P) {
        int indexP = 0;
        for (double componentToSum : P.components) {
            components.set(indexP, components.get(indexP) + componentToSum);
            indexP++;
            summedPoints += P.summedPoints;
        }
    }

    // Aggiunge alla lista delle componenti quelle di P
    public void inizializeCopy(Point P) {
        int indexP = 0;
        for (double component : P.components) {
            components.add(indexP, component);
            indexP++;
        }
    }

    // Copia componente per componente
    public void copy(Point P) {
        int indexP = 0;
        for (double component : P.components) {
            components.set(indexP, component);
            indexP++;
        }
        indexP = P.index;
        summedPoints = P.summedPoints;
    }

    // Calcola il baricentro e setta i componenti con i suoi valori
    public void computeAndSetBarycenter() {
        int pointIndex = 0;
        for (double component : components) {
            components.set(pointIndex, component / summedPoints);
            pointIndex++;
        }
    }

    // Deserializzazione di un punto da una stringa, STATICA
    public static Point deserialize(String s) throws IOException, ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        Point point;
        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(data))) {
            point = (Point) ois.readObject();
        }
        return point;
    }

    // Serializzazione di un punto in una stringa, STATICA
    public String serialize() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(this);
        } catch (IOException ex) {
            System.err.println("Error in converting to string: " + ex.getMessage());
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }

    // Point to String
    @Override
    public String toString() {
        return index + " " + components.toString();
    }

}
