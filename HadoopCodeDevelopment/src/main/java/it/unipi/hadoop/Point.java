package it.unipi.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Objects;
import static java.util.Objects.hash;
import org.apache.hadoop.io.WritableComparable;

// Classe che rappresenta un qualsiasi punto
public class Point implements WritableComparable, Serializable {

    // Componenti del punto
    public ArrayList<Double> components = new ArrayList<>();
    // Per le somme parziali, numero di punti sommati
    public int summedPoints = 1;
    // -1 se è un punto qualsiasi, altro se indice di un centroide
    public int index = -1;
    // Number of components
    public int dimensions = 1;

 
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
        }
        summedPoints += P.summedPoints;  
    }
    
    // Setta il punto attraverso il punto passato
    void set(Point P) {
        components.clear();
        P.components.forEach((component) -> {
            components.add(component);
        });
        index = P.index;
        dimensions = P.dimensions;
        summedPoints = P.summedPoints;
    }

    // Calcola il baricentro e setta i componenti con i suoi valori
    public void computeAndSetBarycenter() {
        int pointIndex = 0;
        for (double component : components) {
            components.set(pointIndex, component / summedPoints);
            pointIndex++;
        }
        summedPoints = 1;
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
        return "idex:"+index + "-" +"summedPoints:"+summedPoints + "-" +"dimensions:"+dimensions + "-" + components.toString().replaceAll(" ", "");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(index);
        out.writeInt(summedPoints);
        out.writeInt(dimensions);
        for (int i = 0; i < dimensions; i++) {
            out.writeDouble(components.get(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        components.clear();
        index = in.readInt();
        summedPoints = in.readInt();
        dimensions = in.readInt();
        for (int i = 0; i < dimensions; i++) {
            components.add(in.readDouble());
        }
    }

    @Override // Quando inserito come chiave, serve per comparare le chiavi
    public int compareTo(Object o) {
        Point p = (Point) o;
        // Solo centroidi vengono inseriti come chiave, perciò avranno indici diversi
        return (this.index < p.index ? -1 : (this.index == p.index ? 0 : 1));
    }

    @Override
    public int hashCode() {
        return hash(components,index,summedPoints,dimensions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Point other = (Point) obj;
        return Objects.equals(this.components, other.components);
    }

}
