import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            HashMap<Integer, Integer> mapWords = new HashMap<Integer, Integer>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                int len = itr.nextToken().length();
                if(mapWords.containsKey(len)){
                    mapWords.put(len, mapWords.get(len) + 1);
                }else{
                    mapWords.put(len, 1);
                }
            }

            for (Map.Entry<Integer, Integer> IntegerEntry : mapWords.entrySet()) {
                context.write(
                        new IntWritable((IntegerEntry).getKey()),
                        new IntWritable((IntegerEntry).getValue()));
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
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
        long a=System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        if(job.waitForCompletion(true)){
            System.out.println("\r执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
            System.exit(0);
        }else{
            System.out.println("\r执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
            System.exit(1);
        }
    }
}
