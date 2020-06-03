package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Il programma deve essere strutturato con una classe base + sottoclassi per mapper, reducer e opzionalmente combiner
public class KMeans {

    // Sottoclasse che implemeta il codice e le variabili del mapper
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final IntWritable outputKey = new IntWritable();
        private final Text outputValue = new Text();

        // codice del mapper
        @Override
        //Uno per riga del file di input!
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            System.out.println("DEBUG | Mapper ID: " + context.getJobID());

            // Il mapper prende SEMPRE in ingresso un file e lo scorre riga per riga
            // key = indice della riga del file
            // value = riga del file
            // Preleva la struttura che contiene la configurazione
            Configuration conf = context.getConfiguration();
            ArrayList<PointWritable> centroids = new ArrayList<>();
            int k = Integer.parseInt(conf.get("k"));
            //int d = conf.getInt("d", 0);

            // Lettura dei centroidi *nello stesso ordine* in cui sono stati passati
            int index = 0;
            while (index < k) {
                //System.out.println("DEBUG | Mapper " + context.getJobID() + " centroid " + index + " string:" + conf.get("centroid-" + index));
                try {
                    centroids.add(index, PointWritable.deserialize(conf.get("centroid-" + index)));
                    System.out.println("DEBUG | Mapper " + context.getJobID() + " centroid  :" + centroids.get(index));
                } catch (ClassNotFoundException ex) {
                    System.err.println("A problem occurred in passing the centroids: " + ex.getMessage());
                }
                index++;
            }

            // Legge la riga dal file di input
            // x, y, z...
            String line = value.toString();
            PointWritable point = new PointWritable();
            for (String component : line.split(",")) {
                point.components.add(Double.valueOf(component));
            }
            System.out.println("DEBUG | Mapper " + context.getJobID() + " point: " + point.toString());

            double minDistance = 2; //Ipotizzo punti con componenti comprese tra 0 e 1
            for (PointWritable centroid : centroids) {
                double distance = PointWritable.distance(2, centroid, point);
                if (distance < minDistance) {
                    minDistance = distance;
                    outputKey.set(centroid.index);
                }
                System.out.println("DEBUG | Mapper " + context.getJobID() + " distance from centroid " + centroid.toString() + " = " + distance);
            }

            outputValue.set(point.serialize());

            System.out.println("DEBUG | Mapper " + context.getJobID() + " <Key,Value>: <" + outputKey + "," + outputValue.toString() + ">");

            // inserisce la coppia chiave-valore nel contesto
            context.write(outputKey, outputValue);

        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final IntWritable outputKey = new IntWritable();
        private final Text outputValue = new Text();

        // codice del combiner
        @Override
        //Un combiner non ha una classe propria ma può essere implementato come reducer
        public void reduce(IntWritable key, Iterable<Text> points, Context context) throws IOException, InterruptedException {

            System.out.println("DEBUG | Combiner ID: " + context.getJobID());

            outputKey.set(key.get());
            System.out.println("DEBUG | Combiner " + context.getJobID() + " Key: " + outputKey);

            boolean first = true;
            PointWritable point = null;
            PointWritable sumPoint = null;
            while (points.iterator().hasNext()) {
                try {
                    point = PointWritable.deserialize(points.iterator().next().toString());
                } catch (ClassNotFoundException ex) {
                    System.err.println("A problem occurred in passing the point to combiner: " + ex.getMessage());
                }
                System.out.println("DEBUG | Combiner " + context.getJobID() + " point: " + point.toString());
                if (first == true) {
                    sumPoint = point;
                } else {
                    sumPoint.sum(point);
                }
                first = false;
            }
            outputValue.set(sumPoint.serialize());
            System.out.println("DEBUG | Combiner " + context.getJobID() + " <Key,Value>: <" + outputKey + "," + sumPoint.toString() + ">");

            context.write(outputKey, outputValue);
        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        private final IntWritable outputKey = new IntWritable();
        private final Text outputValue = new Text();

        // codice del reducer
        @Override
        //Uno per chiave!
        public void reduce(IntWritable key, Iterable<Text> points, Context context) throws IOException, InterruptedException {

            System.out.println("DEBUG | Reducer ID: " + context.getJobID());
            Configuration conf = context.getConfiguration();
            outputKey.set(key.get());

            PointWritable newCentroid = null;
            PointWritable point = null;
            boolean first = true;
            while (points.iterator().hasNext()) {
                try {
                    point = PointWritable.deserialize(points.iterator().next().toString());
                } catch (ClassNotFoundException ex) {
                    System.err.println("A problem occurred in passing the point to combiner: " + ex.getMessage());
                }
                if (first == true) {
                    newCentroid = point;
                    newCentroid.index = key.get();
                } else {
                    newCentroid.sum(point);
                }
                first = false;
            }
            newCentroid.computeAndSetBarycenter();

            System.out.println("DEBUG | Reducer " + context.getJobID() + " <Key,Value>: <" + outputKey + "," + outputValue.toString() + ">");
            outputValue.set(newCentroid.toString());
            // Scrivo il nuovo valore del centroide nel contesto per essere letto dalla prossima iterazione
            conf.set("centroid-" + key.get(), newCentroid.serialize());
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {

        // Struttura che contiene la configurazione
        Configuration conf = new Configuration();

        // Preleva tutti gli argomenti
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Errore se il numero di argomenti è diverso da quello previsto
        if (otherArgs.length != 4) {
            System.err.println("Usage: KMeans <input points> <d> <k> <output clusters centroids>");
            System.exit(1);
        }

        // Print degli argomenti da riga di comando
        System.out.println("args[0]: <input points>=" + otherArgs[0]); // FIle di input
        System.out.println("args[1]: <d>=" + otherArgs[1]); // Numero componenti per punto
        System.out.println("args[2]: <k>=" + otherArgs[2]); // Numero classi
        System.out.println("args[3]: <output cluster centroids>=" + otherArgs[3]); // File di output

        ArrayList<PointWritable> centroids = new ArrayList<>();
        int i = 0;
        while (i < Integer.parseInt(otherArgs[2])) {
            PointWritable centroid = new PointWritable();
            centroid.index = i;
            int j = 0;
            while (j < Integer.parseInt(otherArgs[1])) {
                centroid.components.add(Math.random()); //Ipotizzo punti con componenti comprese tra 0 e 1 (bata standardizzarli)
                j++;
            }
            System.out.println("DEBUG | Centroid " + centroid.index + " initial components: " + centroid.components);
            centroids.add(centroid);
            i++;
        }

        // Assegna la classe del job
        Job job = Job.getInstance(conf, "KMeans");

        // Inserisce nella struttura di configurazione i nomi dei campi e i loro valori
        job.getConfiguration().set("d", otherArgs[1]);
        job.getConfiguration().set("k", otherArgs[2]);

        for (PointWritable centroid : centroids) {
            //System.out.println("DEBUG | Centroid to String: " + centroid.serialize());
            job.getConfiguration().set("centroid-" + centroid.index, centroid.serialize());
            /*try {
                System.out.println("DEBUG | Centroid from String: " + PointWritable.deserialize(centroid.serialize()).components);
            } catch (IOException ex) {
                System.out.println("Errore io");
            } catch (ClassNotFoundException ex) {
                System.out.println("Errore classe");
            }*/
        }

        // Carica la classe base
        job.setJarByClass(KMeans.class);

        // Carica la sottoclasse del mapper
        job.setMapperClass(KMeansMapper.class);
        // Carica la sottoclasse del reducer
        job.setCombinerClass(KMeansCombiner.class);
        // Carica la sottoclasse del reducer
        job.setReducerClass(KMeansReducer.class);

        // I reducer sono limitati a 3 per volta
        //job.setNumReduceTasks(3);
        // define mapper's output key-value format
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's and combiner's output key-value format
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //File che il mapper legge riga per riga
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)) {
            int n = 0;
            while (i < Integer.parseInt(otherArgs[2])) {
                try {
                    System.out.println("DEBUG | Centroid " + n + " : " + PointWritable.deserialize(job.getConfiguration().get("centroid-" + n)).components);
                } catch (IOException ex) {
                    System.out.println("Errore io");
                } catch (ClassNotFoundException ex) {
                    System.out.println("Errore classe");
                }
                n++;
            }
            System.exit(1);
        }
    }

}
