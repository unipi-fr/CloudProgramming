package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

    // Tipologia di distanza tra i punti, di default Euclidea
    public static final int distanceType = 2;

    // Sottoclasse che implemeta il codice e le variabili del mapper
    public static class KMeansMapper extends Mapper<LongWritable, Text, Point, Point> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final Point outputKey = new Point();
        private final Point outputValue = new Point();

        // codice del mapper
        @Override
        //Uno per riga del file di input
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //System.out.println("DEBUG | Mapper start ----------");
            // Preleva la struttura che contiene la configurazione
            Configuration conf = context.getConfiguration();
            ArrayList<Point> centroids = new ArrayList<>();
            int k = Integer.parseInt(conf.get("k"));
            int d = Integer.parseInt(conf.get("d"));

            // Lettura dei centroidi
            for (int index = 0; index < k; index++) {
                try {
                    // I centroidi vengono passati come stringhe nella configurazione serializzandoli e poi deserializzandoli qua
                    centroids.add(index, Point.deserialize(conf.get("centroid-" + index)));
                    //System.out.println("DEBUG | Mapper, centroid  :" + centroids.get(index));
                } catch (ClassNotFoundException ex) {
                    System.err.println("A problem occurred in passing the centroids: " + ex.getMessage());
                }
            }

            // Il mapper prende SEMPRE in ingresso un file e lo scorre riga per riga
            // key = indice della riga del file
            // value = riga del file
            // una riga per mapper
            // x,y,z ...
            String line = value.toString();

            // Inizializza i componenti del punto
            Point point = new Point();
            for (String component : line.split(",")) {
                point.components.add(Double.valueOf(component));
            }
            point.dimensions = d;
            //System.out.println("DEBUG | Mapper, point: " + point.toString());

            // Calcola qual è il centroide più vicino a quel punto
            // Setta di conseguenza la chiave e il valore in uscita
            double minDistance = Double.MAX_VALUE;
            for (Point centroid : centroids) {
                double distance = Point.distance(distanceType, centroid, point);
                if (distance < minDistance) {
                    minDistance = distance;
                    outputKey.set(centroid);
                }
                //System.out.println("DEBUG | Mapper, distance from centroid " + centroid.toString() + " = " + distance);
            }
            // Per essere passato correttamente il punto deve essere deserializzato e passato come stringa
            outputValue.set(point);
            //System.out.println("DEBUG | Mapper, <Key,Value>: <" + outputKey + "," + outputValue.toString() + ">");

            // inserisce la coppia chiave-valore nel contesto
            // <indice del centroide più vicino, punto>
            context.write(outputKey, outputValue);

        }
    }

    // Sottoclasse che implemeta il codice e le variabili del combiner
    public static class KMeansCombiner extends Reducer<Point, Point, Point, Point> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final Point outputKey = new Point();
        private final Point outputValue = new Point();

        // codice del combiner
        @Override
        //Un combiner non ha una classe propria ma può essere implementato come reducer
        // Il combiner riceve una lista di punti associati ad un centroide e ne calcola la somma parziale
        public void reduce(Point key, Iterable<Point> points, Context context) throws IOException, InterruptedException {

            //System.out.println("DEBUG | Combiner start ----------");
            // La chiave in input, l'indice del centroide, è la stessa che va in output
            outputKey.set(key);
            //System.out.println("DEBUG | Combiner, Key: " + outputKey);

            // Deserializza i punti ricevuti nella lista di valori in input
            // e calcola contemporaneamente la somma parziale delle componenti
            boolean first = true;
            Point point = new Point();
            Point sumPoint = new Point();
            for (Point p : points) {
                point.set(p);
                //System.out.println("DEBUG | Combiner, point: " + point.toString());
                if (first == true) { // Se è il primo
                    sumPoint.set(point);
                    first = false;
                } else { // Altrimenti somma le componenti
                    sumPoint.sum(point);
                }
            }

            // Serializza il punto che contiene la somma parziale delle componenti per essere passato come value
            outputValue.set(sumPoint);
            //System.out.println("DEBUG | Combiner, <Key,Value>: <" + outputKey + "," + sumPoint.toString() + ">");

            // inserisce la coppia chiave-valore nel contesto
            // <indice del centroide, somma parziale punti assegnati ad esso>
            context.write(outputKey, outputValue);
        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansReducer extends Reducer<Point, Point, Point, Text> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final Point outputKey = new Point();
        private final Text outputValue = new Text();

        // Codice del reducer
        // Un reducer per chiave
        @Override
        public void reduce(Point key, Iterable<Point> points, Context context) throws IOException, InterruptedException {

            //System.out.println("DEBUG | Reducer start ----------");
            // La chiave in input, l'indice del centroide, è la stessa che va in output
            outputKey.set(key);

            // Deserializza i punti con le somme parziali ricevuti nella lista di valori in input
            // e calcola contemporaneamente la somma di tutte le componenti
            Point newCentroid = new Point();
            Point point = new Point();
            boolean first = true;
            for (Point p : points) {
                point.set(p);
                //System.out.println("DEBUG | Reducer, partial sum " + point);
                if (first == true) {
                    newCentroid.set(point);
                    // Setta l'indice del nuovo centroide come quello del suo predecessore
                    newCentroid.index = key.index;
                    first = false;
                } else {
                    newCentroid.sum(point);
                }
            }

            // Media componente per componente partendo dall somma delle componenti e dal numero di punti sommati
            newCentroid.computeAndSetBarycenter();

            //System.out.println("DEBUG | Reducer, <Key,Value>: <" + outputKey + "," + outputValue.toString() + ">");
            // Serializza il nuovo centroide come valore in output
            outputValue.set(newCentroid.serialize());

            // <indice del centroide, nuovo centroide>
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {

        // Per calcolare le performance
        Instant start = Instant.now();

        // Struttura che contiene la configurazione
        Configuration conf = new Configuration();

        // Per poter effettuare operazioni sull'HDFS
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));

        // Preleva tutti gli argomenti
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Errore se il numero di argomenti è diverso da quello previsto
        if (otherArgs.length != 6) {
            System.err.println("Usage: KMeans <input points> <d> <k> <stop_margin> <max_iterations> <output clusters centroids>");
            System.exit(1);
        }

        int k = Integer.parseInt(otherArgs[2]);
        int d = Integer.parseInt(otherArgs[1]);

        // Criterio di stop
        double stopCriteria = Double.valueOf(otherArgs[3]) * k;
        // Numero massimo di iterazioni in caso di convergenza lenta
        int maxIterations = Integer.parseInt(otherArgs[4]); //per sicurezza

        // Il criterio di stop è la somma delle distanze dei centroidi da quelli al passo precedente
        // Inpostato ad un valore superiore al criterio di stop
        double centroidsMovementFactor = stopCriteria + 1;

        // Print degli argomenti da riga di comando
        System.out.println("INFO | args[0]: <input points> = " + otherArgs[0]); // File di input
        System.out.println("INFO | args[1]: <d> = " + d); // Numero componenti per punto
        System.out.println("INFO | args[2]: <k> = " + k); // Numero centroidi = numero cluster
        System.out.println("INFO | Stop Criteria = " + stopCriteria); 
        System.out.println("INFO | args[4]: <max_iterations> = " + maxIterations); 
        System.out.println("INFO | args[5]: <output cluster centroids> = " + otherArgs[5]); // Cartella di output
       
        // Array dei centroidi
        ArrayList<Point> centroids = new ArrayList<>();

        /* --- Scelta casuale dei centroidi ---
        // Inizialmente vengono scelte componenti casuali comprese tra 0 e 1 (basta standardizzarli)
        for (int centroidIndex = 0; centroidIndex < k; centroidIndex++) {
            Point centroid = new Point();
            // Indice del centroide
            centroid.index = centroidIndex;
            centroid.dimensions = d;
            for (int componentIndex = 0; componentIndex < d; componentIndex++) {
                centroid.components.add(Math.random()); //Ipotizzo punti con componenti comprese tra 0 e 1 (basta standardizzarli)
            }
            System.out.println("INFO | Centroid " + centroid.index + " initial components: " + centroid.components);
            centroids.add(centroid);
        } */
        
        // Scelgo k centroidi casuali dal dataset usando il metodo della "reservoir list"
        Path path = new Path(otherArgs[0]);
        FileSystem fs = path.getFileSystem(conf);
        String currentLine = null;
        
        // reservoirList è la lista delle linee selezionate
        ArrayList<String> reservoirList = new ArrayList<>(k);
        int count = 0;

        Random ra = new Random();
        int randomNumber = 0;
        Scanner sc = new Scanner(fs.open(path)).useDelimiter("\n");

        // inizialmente ne aggiunge k poi con probabilità decrescente li sostituisce con una linea casuale del file
        while (sc.hasNext()) {
            currentLine = sc.next();
            count++;
            if (count <= k) {
                reservoirList.add(currentLine);
            } else if ((randomNumber = (int) ra.nextInt(count)) < k) {
                reservoirList.set(randomNumber, currentLine);
            }
        }
        
        for (int centroidIndex = 0; centroidIndex < k; centroidIndex++) {
            Point centroid = new Point();
            // Indice del centroide
            centroid.index = centroidIndex;
            centroid.dimensions = d;
            for (String component : reservoirList.get(centroidIndex).split(",")) {
                centroid.components.add(Double.valueOf(component));
            }
            System.out.println("INFO | Centroid " + centroid.index + " initial components: " + centroid.components);
            centroids.add(centroid);
        }

        // Loop di map-reduce fino a soddisfacimento criterio di stop o limite iterazioni
        for (int jobIndex = 0; jobIndex < maxIterations && centroidsMovementFactor > stopCriteria; ++jobIndex) {

            System.out.println("INFO | Job " + (jobIndex + 1) + " is running...");

            // Assegna la configurazione al job
            Job job = Job.getInstance(conf, "KMeans");

            // Inserisce nella struttura di configurazione k e d
            job.getConfiguration().set("d", otherArgs[1]);
            job.getConfiguration().set("k", otherArgs[2]);

            // Passa i centroidi come configurazione serializzandoli in stringhe
            centroids.forEach((centroid) -> {
                job.getConfiguration().set("centroid-" + centroid.index, centroid.serialize());
            });

            // Carica la classe base
            job.setJarByClass(KMeans.class);

            // Carica la sottoclasse del mapper
            job.setMapperClass(KMeansMapper.class);
            // Carica la sottoclasse del combiner
            job.setCombinerClass(KMeansCombiner.class);
            // Carica la sottoclasse del reducer
            job.setReducerClass(KMeansReducer.class);

            // Il numero consigliato dalla documentazione è:
            // 1.75 * (nodes * mapred.tasktracker.reduce.tasks.maximum)
            // 2 è il numero di container generato di default
            job.setNumReduceTasks(14); //(int) (1.75 * (4.0 * 2.0))
            
            // Definisce i formati key-value del mapper e combiner
            job.setMapOutputKeyClass(Point.class);
            job.setMapOutputValueClass(Point.class);

            // Definisce i formati key-value del reducer
            job.setOutputKeyClass(Point.class);
            job.setOutputValueClass(Text.class);

            // Definisce i file di input e output
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //File che il mapper legge riga per riga
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[5] + "_" + jobIndex));

            // Definisce i formati di input e output
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // Aspetta finisca il map-reduce e lo notifica
            System.out.println("INFO | Job " + (jobIndex + 1) + " completato con: " + job.waitForCompletion(true));

            // Legge il risultato del precedente job e preleva i centroidi
            // Se qalche centroide non aveva punti assegnati non viene inserito nell'output
            // in quanto non arriva a nessun reducer, non è un problema perchè rimane il
            // valore che aveva alla vecchia iterazione
            path = new Path(otherArgs[5] + "_" + jobIndex);
            fs = path.getFileSystem(conf);
            FileStatus[] fss = fs.listStatus(path);
            boolean first = true;
            // Itera tutti i file dentro la cartella di output prelevando il contenuto
            for (FileStatus status : fss) {
                // Salta il primo perchè non è di output
                if (!first) {
                    path = status.getPath();
                    BufferedReader bufRead = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line = "";
                    while (line != null) {
                        line = bufRead.readLine();
                        if (line != null && !"null".equals(line)) {
                            Point newCentroid = Point.deserialize(line.split("\\s+")[1]);
                            centroids.set(newCentroid.index, newCentroid);
                        }
                    }
                } else {
                    first = false;
                }
            }

            // Elimina l'output precedente e rinomina quello attuale in modo da avere una sola cartella
            fs.delete(new Path(otherArgs[5]), true);
            fs.rename(new Path(otherArgs[5] + "_" + jobIndex), new Path(otherArgs[5]));

            // Calcola la somma delle distanze tra le vecchie posizioni dei centroidi e le nuove
            centroidsMovementFactor = 0;
            for (int centroidIndex = 0; centroidIndex < k; centroidIndex++) {
                Point oldCentroid = Point.deserialize(job.getConfiguration().get("centroid-" + centroidIndex));
                System.out.println("INFO | New centroid : " + centroids.get(centroidIndex));
                System.out.println("INFO | Old centroid : " + oldCentroid);
                centroidsMovementFactor += Point.distance(distanceType, oldCentroid, centroids.get(centroidIndex));
            }
            System.out.println("INFO | Centroid Movement Factor = " + centroidsMovementFactor);
            System.out.println("INFO | job " + (jobIndex + 1) + " concluded in " + Duration.between(start, Instant.now()).getSeconds() + " seconds from start");
        }

        // Convergenza e conclusione algoritmo
        System.out.println("INFO | Algorithm completed in " + Duration.between(start, Instant.now()).getSeconds() + " seconds -------------------------------------------------------");
        for (int centroidIndex = 0; centroidIndex < k; centroidIndex++) {
            System.out.println("INFO | Final centroid : " + centroids.get(centroidIndex));
        }
        System.exit(1);
    }

}
