import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class FastaEntropia {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration configuracao = new Configuration();
        Job trabalho = Job.getInstance(configuracao, "fastaentropia");

        trabalho.setMapOutputKeyClass(Text.class);
        trabalho.setMapOutputValueClass(MapWritable.class);
        trabalho.setCombinerClass(CombinerFastaEntropia.class);
        trabalho.setReducerClass(ReduceFasta.class);
        trabalho.setOutputKeyClass(Text.class);
        trabalho.setOutputValueClass(DoubleWritable.class);
        trabalho.setJarByClass(FastaEntropia.class);
        trabalho.setMapperClass(MapFasta.class);
        FileInputFormat.addInputPath(trabalho, new Path("in/JY157487.1.fasta"));
        FileOutputFormat.setOutputPath(trabalho, new Path("output"));
        System.exit(trabalho.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapFasta extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable chave, Text valor, Context contexto) throws IOException, InterruptedException {
            String linha = valor.toString();

            if (linha.startsWith(">")) return;

            MapWritable frequencias = new MapWritable();
            for (char caracter : linha.toCharArray()) {
                if (caracter == 'A' || caracter == 'C' || caracter == 'T' || caracter == 'G') {
                    Text charTexto = new Text(String.valueOf(caracter));
                    LongWritable contagem = frequencias.containsKey(charTexto) ? (LongWritable) frequencias.get(charTexto) : new LongWritable(0);
                    contagem.set(contagem.get() + 1);
                    frequencias.put(charTexto, contagem);
                }
            }
            contexto.write(new Text("caracter"), frequencias);
        }
    }

    public static class CombinerFastaEntropia extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text chave, Iterable<MapWritable> valores, Context contexto) throws IOException, InterruptedException {
            MapWritable resultado = new MapWritable();

            for (MapWritable frequencias : valores) {
                for (Map.Entry<Writable, Writable> entrada : frequencias.entrySet()) {
                    Text charTexto = (Text) entrada.getKey();
                    LongWritable contagem = (LongWritable) entrada.getValue();
                    LongWritable contagemExistente = resultado.containsKey(charTexto) ? (LongWritable) resultado.get(charTexto) : new LongWritable(0);
                    contagemExistente.set(contagemExistente.get() + contagem.get());
                    resultado.put(charTexto, contagemExistente);
                }
            }

            contexto.write(chave, resultado);
        }
    }

    public static class ReduceFasta extends Reducer<Text, MapWritable, Text, Text> {
        public void reduce(Text chave, Iterable<MapWritable> valores, Context contexto) throws IOException, InterruptedException {
            Map<Character, Long> contagemCaracteres = new HashMap<>();
            long totalCaracteres = 0;

            for (MapWritable frequencias : valores) {
                for (Map.Entry<Writable, Writable> entrada : frequencias.entrySet()) {
                    Text charTexto = (Text) entrada.getKey();
                    LongWritable contagem = (LongWritable) entrada.getValue();
                    totalCaracteres += contagem.get();
                    contagemCaracteres.put(charTexto.toString().charAt(0), contagem.get());
                }
            }

            for (Map.Entry<Character, Long> entrada : contagemCaracteres.entrySet()) {
                char caracter = entrada.getKey();
                long contagem = entrada.getValue();
                double probabilidade = (double) contagem / totalCaracteres;
                double entropia = -probabilidade * (Math.log(probabilidade) / Math.log(2));

                String resultado = "Contagem: " + contagem + ", Entropia: " + entropia;
                contexto.write(new Text(String.valueOf(caracter)), new Text(resultado));
            }
        }
    }
}
