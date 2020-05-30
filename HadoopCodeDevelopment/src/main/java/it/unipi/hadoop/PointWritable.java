package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public class PointWritable implements Writable {

    public ArrayList<Double> components = new ArrayList<>();

    public PointWritable() {}

    // Distanza di tipologia distanceType tra due punti
    public static double distance(int distanceType, PointWritable P1, PointWritable P2) {
        double sum = 0;
        int index = 0;
        for (double componentP1 : P1.components) {
            double componentP2 = P2.components.get(index);
            sum += Math.pow((componentP1 + componentP2), distanceType);
            index++;
        }
        return Math.pow(sum, 1.0 / distanceType);
    }

    // Somma componente per componente
    public void sum(PointWritable P) {
        int index = 0;
        for (double componentToSum : P.components) {
            components.add(index, components.get(index) + componentToSum);
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

    @Override
    public String toString() {
        return components.toString();
    }

}
