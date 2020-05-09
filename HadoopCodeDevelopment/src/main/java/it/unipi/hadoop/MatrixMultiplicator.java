package it.unipi.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

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
public class MatrixMultiplicator {

   // Sottoclasse che implemeta il codice e le variabili del mapper
   public static class MatrixMultiplicatorMapper extends Mapper<LongWritable, Text, Text, Text> {
   
        // IMPORTANTE: usare delle variabili "final" per passare la chiave e il valore al context!
        private final Text outputKey   = new Text();
        private final Text outputValue = new Text();
        
         // codice del mapper
         @Override
         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            // Il mapper prende SEMPRE in ingresso un file e lo scorre riga per riga
            // key = indice della riga del file
            // value = riga del file
            
            // Preleva la struttura che contiene la configurazione
            Configuration conf = context.getConfiguration();
            // Preleva i singoli campi di configurazione
            final int NUM_COLS_N = Integer.parseInt(conf.get("k"));
            final int NUM_ROWS_M = Integer.parseInt(conf.get("i"));
            
            // Legge la riga dal file di input
            String line = value.toString();

            // (M, i, j, Mij);
            String[] indicesAndValue = line.split(",");

            if (indicesAndValue[0].equals("M")) {
               for (int k = 0; k < NUM_COLS_N; k++) {
                  // setta la chiave in output 
                  outputKey.set(indicesAndValue[1] + "," + k);
                  // setta il valore in output 
                  outputValue.set("M" + "," + indicesAndValue[2] + "," + indicesAndValue[3]);
                  // inserisce la coppia chiave-valore nel contesto
                  context.write(outputKey, outputValue);
               }
            } else {
               // (N, j, k, Njk);
               for (int i = 0; i < NUM_ROWS_M; i++) {
                  // setta la chiave in output 
                  outputKey.set(i + "," + indicesAndValue[2]);
                  // setta il valore in output 
                  outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]);
                  // inserisce la coppia chiave-valore nel contesto
                  context.write(outputKey, outputValue);
               }
            }
         }
    }

    // Sottoclasse che implemeta il codice e le variabili del reducer
    public static class MatrixMultiplicatorReducer extends Reducer<Text, Text, NullWritable, Text>{

        // codice del reducer
        @Override
        //Uno per chiave (i,j) !!!!!!!
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // j = colonne di M o righe di N
            final int SHARED_SIZE = Integer.parseInt(context.getConfiguration().get("j"));
            
            // key=(i,k),
            Map<Integer, Float> valuesOfM = new HashMap<>();
            Map<Integer, Float> valuesOfN = new HashMap<>();
            
            // Values = [(M|N,j,Mij|Njk),..]
            String[] value;
            //values è un iterable con tutti i valori (in una lista) relativi ad una chiave
            for (Text val : values) {
                  value = val.toString().split(",");
                  if (value[0].equals("M"))
                     valuesOfM.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                  else
                     valuesOfN.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
            }
            
            float result = 0.0f;
            float m_ij;
            float n_jk;
            
            for (int j = 0; j < SHARED_SIZE; j++) {
                  m_ij = valuesOfM.containsKey(j) ? valuesOfM.get(j) : 0.0f;
                  n_jk = valuesOfN.containsKey(j) ? valuesOfN.get(j) : 0.0f;
                  result += m_ij * n_jk;
            }
            
            if (result != 0.0f)
                  //write(KEYOUT key, VALUEOUT value)
                  context.write(null, new Text(key.toString() + "," + Float.toString(result)));
         }
    }

      public static void main(String[] args) throws Exception{
         
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
         System.out.println("args[0]: <input matrixes M,N>="+otherArgs[0]);
         System.out.println("args[1]: <i>="+otherArgs[1]);
         System.out.println("args[2]: <j>="+otherArgs[2]);
         System.out.println("args[3]: <k>="+otherArgs[3]);
         System.out.println("args[4]: <output MxN>="+otherArgs[4]);
         
         // Assegna la classe del job
         Job job = Job.getInstance(conf, "MatrixMultiplicator"); 
         
         // Inserisce nella struttura di configurazione i nomi dei campi e i loro valori
         job.getConfiguration().set("i", otherArgs[1]); // i = righe di M
         job.getConfiguration().set("j", otherArgs[2]); // j = colonne di M o righe di N
         job.getConfiguration().set("k", otherArgs[3]); // k = colonne di N

         // Carica la classe base
         job.setJarByClass(MatrixMultiplicator.class);
         
         // Carica la sottoclasse del mapper
         job.setMapperClass(MatrixMultiplicatorMapper.class);
         // Carica la sottoclasse del reducer
         job.setReducerClass(MatrixMultiplicatorReducer.class);

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