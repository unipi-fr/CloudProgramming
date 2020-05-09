package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        // codice del mapper
        @Override
        //Uno per riga del file di input!
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Il mapper prende SEMPRE in ingresso un file e lo scorre riga per riga
            // key = indice della riga del file
            // value = riga del file
            // Preleva la struttura che contiene la configurazione
            Configuration conf = context.getConfiguration();

            // Legge la riga dal file di input
            String line = value.toString();

            // setta la chiave in output 
            outputKey.set("");
            // setta il valore in output 
            outputValue.set("");
            // inserisce la coppia chiave-valore nel contesto
            context.write(outputKey, outputValue);
        }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class KMeansReducer extends Reducer<Text, Text, NullWritable, Text> {

        // codice del reducer
        @Override
        //Uno per chiave!
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            for (Text val : values) {
            }
            context.write(null, null);
        }
    }

    public static void main(String[] args) throws Exception {

        // Struttura che contiene la configurazione
        Configuration conf = new Configuration();

        // Preleva tutti gli argomenti
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Errore se il numero di argomenti è diverso da quello previsto
        if (otherArgs.length != 5) {
            System.err.println("Usage: SortInMemory_MovingAverageDriver <input matrixes M,N> <i> <j> <k> <output MxN>");
            System.exit(1);
        }

        // Print degli argomenti da riga di comando
        System.out.println("args[0]: <input matrixes M,N>=" + otherArgs[0]);
        System.out.println("args[1]: <i>=" + otherArgs[1]);
        System.out.println("args[2]: <j>=" + otherArgs[2]);
        System.out.println("args[3]: <k>=" + otherArgs[3]);
        System.out.println("args[4]: <output MxN>=" + otherArgs[4]);

        // Assegna la classe del job
        Job job = Job.getInstance(conf, "MatrixMultiplicator");

        // Inserisce nella struttura di configurazione i nomi dei campi e i loro valori
        job.getConfiguration().set("i", otherArgs[1]); // i = righe di M
        job.getConfiguration().set("j", otherArgs[2]); // j = colonne di M o righe di N
        job.getConfiguration().set("k", otherArgs[3]); // k = colonne di N

        // Carica la classe base
        job.setJarByClass(MatrixMultiplicator.class);

        // Carica la sottoclasse del mapper
        job.setMapperClass(KMeansMapper.class);
        // Carica la sottoclasse del reducer
        job.setReducerClass(KMeansReducer.class);

        // I reducer sono limitati a 3 per volta
        job.setNumReduceTasks(3);

        // define mapper's output key-value format
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // define reducer's output key-value format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // define I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));  //File che il mapper legge riga per riga
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
