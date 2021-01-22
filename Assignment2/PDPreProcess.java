import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.String.*;

public class PDPreProcess {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
        private Map<String,Map<String, String>> nodeList;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            nodeList = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            if(itr.hasMoreTokens()){
                String nodeId = itr.nextToken();
                if(!nodeList.containsKey(nodeId)){
                    Map<String, String> edge = new HashMap<>();
                    String nextNode = itr.nextToken();
                    if(!nodeList.containsKey(nextNode)){
                        Map<String, String> newEdge = new HashMap<>();
                        nodeList.put(nextNode, newEdge);
                    }
                    edge.put(nextNode, itr.nextToken());
                    nodeList.put(nodeId, edge);
                }else{
                    nodeList.get(nodeId).put(itr.nextToken(), itr.nextToken());
                }
            }
        }
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String,Map<String, String>> nodeEntry : nodeList.entrySet()) {
                MapWritable edgeList = new MapWritable();
                for(Map.Entry<String, String> edgeEntry: nodeEntry.getValue().entrySet()){
                    if(!nodeEntry.getKey().equals(edgeEntry.getKey())){//remove self loop
                        edgeList.put(new Text(edgeEntry.getKey()), new Text(edgeEntry.getValue()));
                    }
                }
                context.write( new Text(nodeEntry.getKey()), edgeList);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, Text> {


        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            PDNodeWritable node = new PDNodeWritable();

            int src = context.getConfiguration().getInt("src", 0);
            if(key.toString().equals(String.valueOf(src))){
                node.setDistance(new IntWritable(0));
            }else{
                node.setDistance(new IntWritable(-1));
            }
            node.setIncomingNode(key);
            MapWritable edgeList = new MapWritable();

            for(MapWritable val : values){
                for(Map.Entry<Writable, Writable> edgeEntry: val.entrySet()){
                    if(!edgeList.containsKey(edgeEntry.getKey())){
                        edgeList.put(edgeEntry.getKey(),edgeEntry.getValue());
                    }
                }
            }
            node.setEdgeList(edgeList);

            context.write(key, new Text(node.toString()));
        }
    }
    public static void preProcess(String inputFile, String outputFile, int src) throws Exception {
        Configuration conf = new Configuration();
        conf.set("src", String.valueOf(src));
        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
//        job.setCombinerClass(NgramCount.IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        job.waitForCompletion(true);
    }

}
