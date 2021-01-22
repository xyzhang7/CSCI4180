import java.io.IOException;
import java.util.*;
import static java.lang.String.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        int N = 0;
        private Queue<String> ngram;
        private HashMap<String, Integer> mapWords;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            ngram = new LinkedList<>();
            mapWords = new HashMap<>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if(N == 0){
                N = context.getConfiguration().getInt("N", 0);
            }
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9]+", " "));
            while (itr.hasMoreTokens()) {
                ngram.add(itr.nextToken());
                if(ngram.size() == N){
                    String ngramString = join(" ", ngram);
                    ngram.remove();
                    if (mapWords.containsKey(ngramString)) {
                        mapWords.put(ngramString, mapWords.get(ngramString) + 1);
                    } else {
                        mapWords.put(ngramString, 1);
                    }
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> StringEntry : mapWords.entrySet()) {
                context.write(
                new Text((StringEntry).getKey()),
                new IntWritable((StringEntry).getValue()));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        Job job = Job.getInstance(conf, "N gram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(NgramCount.TokenizerMapper.class);
//        job.setCombinerClass(NgramCount.IntSumReducer.class);
        job.setReducerClass(NgramCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
