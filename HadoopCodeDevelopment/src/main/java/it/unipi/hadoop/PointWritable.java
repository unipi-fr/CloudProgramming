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
import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable, Serializable {

    public ArrayList<Double> components = new ArrayList<>();
    public int summedPoints = 1;
    public int index = -1; //-1 se è un punto qualsiasi, altro se indice di un centroide

    public PointWritable() {}

    // Distanza di tipologia distanceType tra due punti
    public static double distance(int distanceType, PointWritable P1, PointWritable P2) {
        double sum = 0;
        int index = 0;
        for (double componentP1 : P1.components) {
            double componentP2 = P2.components.get(index);
            sum += Math.pow((componentP1 - componentP2), distanceType);
            index++;
        }
        return Math.pow(sum, 1.0 / distanceType);
    }

    // Somma componente per componente
    public void sum(PointWritable P) {
        int index = 0;
        for (double componentToSum : P.components) {
            components.set(index, components.get(index) + componentToSum);
            index++;
            summedPoints += P.summedPoints;
        }
    }
    
    // Copia componente per componente
    public void inizializeCopy(PointWritable P) {
        int index = 0;
        for (double component : P.components) {
            components.add(index, component);
            index++;
        }
    }
    
    // Copia componente per componente
    public void copy(PointWritable P) {
        int index = 0;
        for (double component : P.components) {
            components.set(index, component);
            index++;
        }
        index = P.index;
        summedPoints = P.summedPoints;
    }
    
    // Calcola il baricentro e setta i componenti con i suoi valori
    public void computeAndSetBarycenter() {
        int index = 0;
        for (double component : components) {
            components.set(index, component / summedPoints);
            index++;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (double component : components) {
            out.writeDouble(component);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (double component : components) {
            components.add(in.readDouble());
        }
    }
 
    // Point from String
    public static PointWritable deserialize( String s ) throws IOException ,
                                                       ClassNotFoundException {
        byte [] data = Base64.getDecoder().decode( s );
        PointWritable point;
        try (ObjectInputStream ois = new ObjectInputStream( 
                new ByteArrayInputStream(  data ) )) {
            point = (PointWritable) ois.readObject();
        }
        return point;
   }
    
    // Point to String
    public String serialize( ) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream( baos )) {
            oos.writeObject( this );
        } catch (IOException ex) {
            System.err.println("Error in converting to string: " + ex.getMessage());
        }
        return Base64.getEncoder().encodeToString(baos.toByteArray()); 
    }
    
    // Point to String
    @Override
    public String toString( ) {
        return index + " " + components.toString();
    }

}
