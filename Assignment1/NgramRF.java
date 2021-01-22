import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.String.*;


public class NgramRF {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
        int N = 0;
        private Queue<String> ngram = new LinkedList<>();
        private List<Map<String, Integer>> data = new ArrayList<>();
        private Map<String, Integer> firstWordList = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            ngram = new LinkedList<>();
            data = new ArrayList<>();
            firstWordList = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String star = "*";
            if(N == 0){
                N = context.getConfiguration().getInt("N", 0);
            }
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z0-9]+", " "));
            while (itr.hasMoreTokens()) {
                ngram.add(itr.nextToken());
                if(ngram.size() == N){
                    String first = ngram.poll();
                    String ngramString = join(" ", ngram);

                    if(firstWordList.containsKey(first)){
                        if(data.get(firstWordList.get(first)).containsKey(ngramString)){
                            data.get(firstWordList.get(first)).put(star, data.get(firstWordList.get(first)).get(star) + 1);
                            data.get(firstWordList.get(first)).put(ngramString, data.get(firstWordList.get(first)).get(ngramString) + 1);
                        }else{
                            data.get(firstWordList.get(first)).put(star, data.get(firstWordList.get(first)).get(star) + 1);
                            data.get(firstWordList.get(first)).put(ngramString, 1);
                        }
                    }else{
                        firstWordList.put(first, data.size());
                        Map<String, Integer> temp = new HashMap<>();
                        temp.put(star, 1);
                        temp.put(ngramString, 1);
                        data.add(temp);
                    }

                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            MapWritable map = new MapWritable();
            for (Map.Entry<String, Integer> StringEntry : firstWordList.entrySet()) {
                for(Map.Entry<String, Integer> mapEntry: data.get(StringEntry.getValue()).entrySet()){
                    map.put(new Text(mapEntry.getKey()), new IntWritable(mapEntry.getValue()));
                }
                context.write( new Text(StringEntry.getKey()), map);
                map.clear();
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private double percent = -1;

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            if(percent <= -1){
                percent = Double.parseDouble(context.getConfiguration().get("percent"));
            }

            double total = 0;
            Text star = new Text("*");

            Map<Text, Integer> list = new HashMap<>();
            for(MapWritable val : values){
                total += ((IntWritable)val.get(star)).get();
                val.remove(star);
                for(Writable ele : val.keySet()){
                    if(list.containsKey((Text)ele)){
                        list.put((Text)ele, list.get((Text)ele) + ((IntWritable)val.get((Text)ele)).get());
                    }else{
                        list.put((Text)ele, ((IntWritable)val.get((Text)ele)).get());
                    }
                }
            }

            for(Text ele : list.keySet()){
                double temp = list.get(ele)/ total;
                if(temp >= percent){
                    result.set(list.get(ele)/ total);
                    context.write(new Text(key.toString() + " " + ele.toString()), result);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        conf.set("percent", args[3]);
        Job job = Job.getInstance(conf, "N gram rf");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
//        job.setCombinerClass(NgramCount.IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}