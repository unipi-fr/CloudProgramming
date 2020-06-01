package it.unipi.hadoop;

import java.io.IOException;
import java.util.ArrayList;

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
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final IntWritable outputKey = new IntWritable();
        private final PointWritable outputValue = new PointWritable();

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
                System.out.println("DEBUG | Mapper " + context.getJobID() +" centroid "+index+" string:" + conf.get("centroid-" + index));
                try {
                    centroids.add(index, PointWritable.fromString(conf.get("centroid-" + index)));
                } catch (ClassNotFoundException ex) {
                    System.err.println("A problem occurred in passing the centroids: " + ex.getMessage());
                }
                index++;
            }

            // Legge la riga dal file di input
            // x, y, z...
            String line = value.toString();
            for (String component : line.split(",")) {
                outputValue.components.add(Double.valueOf(component));
            }

            double minDistance = 2; //Ipotizzo punti con componenti comprese tra 0 e 1
            for (PointWritable centroid : centroids) {
                double distance = PointWritable.distance(2, centroid, outputValue);
                if (distance < minDistance) {
                    minDistance = distance;
                    outputKey.set(centroids.indexOf(centroid));
                }
            }

            // inserisce la coppia chiave-valore nel contesto
            context.write(outputKey, outputValue);

        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansCombiner extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {
        
        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final IntWritable outputKey = new IntWritable();
        private final PointWritable outputValue = new PointWritable();

        // codice del combiner
        @Override
        //Un combiner non ha una classe propria ma può essere implementato come reducer
        public void reduce(IntWritable key, Iterable<PointWritable> points, Context context) throws IOException, InterruptedException {
           
            System.out.println("DEBUG | Combiner ID: " + context.getJobID());
            
            outputKey.set(key.get());
            System.out.println("DEBUG | Combiner "+context.getJobID()+" Key: " + outputKey);
            
            boolean first = true;
            for (PointWritable point : points) {
                System.out.println("DEBUG | Combiner "+context.getJobID()+"point: " + point.toString());
                if (first = true) {
                    outputValue.inizializeCopy(point);
                } else {
                    outputValue.sum(point);
                }
                first = false;
            }
            System.out.println("DEBUG | Combiner "+context.getJobID()+" <Key,Value>: <" + outputKey+","+outputValue.toString()+">");
            context.write(outputKey, outputValue);
        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansReducer extends Reducer<IntWritable, PointWritable, IntWritable, PointWritable> {

        private final IntWritable outputKey = new IntWritable();
        private final PointWritable outputValue = new PointWritable();

        // codice del reducer
        @Override
        //Uno per chiave!
        public void reduce(IntWritable key, Iterable<PointWritable> points, Context context) throws IOException, InterruptedException {
            
            System.out.println("DEBUG | Reducer ID: " + context.getJobID());
            
            outputKey.set(key.get());
            boolean first = true;
            for (PointWritable point : points) {
                if (first = true) {
                    outputValue.inizializeCopy(point);
                } else {
                    outputValue.sum(point);
                }
                first = false;
            }
            outputValue.computeAndSetBarycenter();
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
            int j = 0;
            while (j < Integer.parseInt(otherArgs[1])) {
                centroid.components.add(Math.random()); //Ipotizzo punti con componenti comprese tra 0 e 1 (bata standardizzarli)
                j++;
            }
            System.out.println("DEBUG | Centroid "+i+" initial components: " + centroid.components);
            centroids.add(centroid);
            i++;
        }

        // Assegna la classe del job
        Job job = Job.getInstance(conf, "KMeans");

        // Inserisce nella struttura di configurazione i nomi dei campi e i loro valori
        job.getConfiguration().set("d", otherArgs[1]);
        job.getConfiguration().set("k", otherArgs[2]);
        int index = 0;
        for(PointWritable centroid : centroids){
            System.out.println("DEBUG | Centroid to String: " + centroid.serialize());
            job.getConfiguration().set("centroid-" + index, centroid.serialize());
            try {
                System.out.println("DEBUG | Centroid from String: " + PointWritable.fromString(centroid.serialize()).components);
            } catch (IOException ex) {
                System.out.println("Errore io");
            } catch (ClassNotFoundException ex) {
                System.out.println("Errore classe");
            }
            index++;
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
        job.setNumReduceTasks(3);

        // define mapper's output key-value format
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PointWritable.class);

        // define reducer's and combiner's output key-value format
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PointWritable.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //File che il mapper legge riga per riga
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}