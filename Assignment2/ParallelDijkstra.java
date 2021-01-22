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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;


public class ParallelDijkstra {
    public static enum NodeCounter{
        NUM_OF_NODE
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, PDNodeWritable> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            Text nodeId = new Text(itr.nextToken());
            int distance = Integer.parseInt(itr.nextToken());
            String incomingNode = itr.nextToken();

            MapWritable edgeList = new MapWritable();

            if(distance != -1){
                while(itr.hasMoreTokens()){
                    Text nodeKey = new Text(itr.nextToken());
                    Text nodeDistance = new Text(itr.nextToken());
                    edgeList.put(nodeKey, nodeDistance);
                    PDNodeWritable node = new PDNodeWritable();
                    node.setDistance(new IntWritable(distance + Integer.parseInt(nodeDistance.toString())));
                    node.setIncomingNode(nodeId);
                    MapWritable temp = new MapWritable();
                    //temp.put(new Text("-2"),new Text("-2") );
                    node.setEdgeList(temp);

                    context.write(nodeKey, node);
                }
            }else{
                while(itr.hasMoreTokens()){
                    Text nodeKey = new Text(itr.nextToken());
                    Text nodeDistance = new Text(itr.nextToken());
                    edgeList.put(nodeKey, nodeDistance);
                }
            }
            // node structure
            PDNodeWritable node = new PDNodeWritable();
            node.setDistance(new IntWritable(distance));
            node.setIncomingNode(new Text(incomingNode));

            if(edgeList.isEmpty()){ //node with no edge
                edgeList.put(new Text("-1"),new Text("-1") );
            }
            node.setEdgeList(edgeList);
            context.write(nodeId, node);
        }
    }

    public static class IntSumReducer extends Reducer<Text, PDNodeWritable, Text, Text> {

        public void reduce(Text key, Iterable<PDNodeWritable> values, Context context) throws IOException, InterruptedException {

            PDNodeWritable result = new PDNodeWritable();
            MapWritable edgeList = new MapWritable();
            int distance = -1;
            int originDistance = -1;
            String originIncomingNode = "";
            boolean nodeTransfer = false;

            for(PDNodeWritable node : values){
//                System.out.print("key value:" + key.toString());
//                System.out.println("\tedge list size:" + node.getEdgeList().size());

                if(node.getEdgeList().size() == 0){// distance
                    int temp = node.getDistance().get();
                    if(temp != -1){ //existed
                        if(temp < distance || distance == -1){
                            distance = temp;
                            result.setIncomingNode(new Text(node.getIncomingNode().toString()));
                        }
                    }

                }else{ //node structure
                    if(!node.getEdgeList().containsKey(new Text("-1"))){
                        for(Map.Entry<Writable, Writable> edgeEntry: node.getEdgeList().entrySet()){
                            edgeList.put(edgeEntry.getKey(),edgeEntry.getValue());
                        }
                    }
                    originDistance = node.getDistance().get();
                    originIncomingNode = node.getIncomingNode().toString();
                }

            }

//            System.out.println(result.toString());
            if(distance == -1){
                result.setDistance(new IntWritable(originDistance));
                result.setIncomingNode(new Text(originIncomingNode));

            }else { //distance exists
                if(originDistance == -1){
                    result.setDistance(new IntWritable(distance));
                    context.getCounter(NodeCounter.NUM_OF_NODE).increment(1);
                }else if(distance < originDistance){
                    result.setDistance(new IntWritable(distance));
                    context.getCounter(NodeCounter.NUM_OF_NODE).increment(1);
                }else{
                    result.setDistance(new IntWritable(originDistance));
                    result.setIncomingNode(new Text(originIncomingNode));
                }
            }
            result.setEdgeList(edgeList);

//            result.setDistance(new IntWritable(distance));
            context.write(key, new Text(result.toString()));
        }
    }

    public static class outMap extends Mapper<Object, Text, Text, PDNodeWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String args[] = value.toString().split("\\s+");
            int distance = Integer.parseInt(args[1]);
            if(distance != -1){
                PDNodeWritable node = new PDNodeWritable(new IntWritable(distance), new Text(args[2]));
                context.write(new Text(args[0]), node);
            }
        }
    }

    public static class outReduce extends Reducer<Text, PDNodeWritable, Text, PDNodeWritable>{
        public void reduce(Text key, Iterable<PDNodeWritable>values, Context context)throws IOException, InterruptedException{
            PDNodeWritable node = new PDNodeWritable();
            for(PDNodeWritable v: values){
                node = v;
            }
            context.write(key, node);
        }
    }

    public static Job setDijkstraJob(Configuration conf, int i) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "iteration-" + i);
        job.setJarByClass(ParallelDijkstra.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PDNodeWritable.class);
        //        job.setCombinerClass(NgramCount.IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hty/q2/tmp/iteration-"+String.valueOf(i) + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hty/q2/tmp/iteration-"+String.valueOf(i+1)));
        return job;
    }

    public static void main(String[] args) throws Exception {
        //Configuration
        Configuration conf = new Configuration();
        String outputPath = args[1];
        int src = Integer.parseInt(args[2]);
        int iterations = Integer.parseInt(args[3]);

        //PreProcess
        PDPreProcess.preProcess(args[0], "/hty/q2/tmp/iteration-0", src);
        int i=0;
        if(iterations > 0){
            for(i = 0; i < iterations;i++){
                Job job = setDijkstraJob(conf, i);
                job.waitForCompletion(true);
                if(job.getCounters().findCounter(ParallelDijkstra.NodeCounter.NUM_OF_NODE).getValue() == 0){
                    break;
                }
            }
        }else if(iterations == 0){
            while(true){
                Job job = setDijkstraJob(conf, i);
                job.waitForCompletion(true);
                if(job.getCounters().findCounter(ParallelDijkstra.NodeCounter.NUM_OF_NODE).getValue() == 0){
                    break;
                }
                i++;
            }
        }else{
//            System.out.println("Iteration should be equal or larger than 0");
            System.exit(0);
        }

        Job outJob = new Job(conf, "Dijkstra");
        outJob.setJarByClass(ParallelDijkstra.class);
        outJob.setMapperClass(outMap.class);
        outJob.setMapOutputKeyClass(Text.class);
        outJob.setMapOutputValueClass(PDNodeWritable.class);
        //        job.setCombinerClass(NgramCount.IntSumReducer.class);
        outJob.setReducerClass(outReduce.class);
        outJob.setOutputKeyClass(Text.class);
        outJob.setOutputValueClass(PDNodeWritable.class);
        FileInputFormat.addInputPath(outJob, new Path("/hty/q2/tmp/iteration-"+String.valueOf(i) + "/part-r-00000"));
        FileOutputFormat.setOutputPath(outJob, new Path(outputPath));
        outJob.waitForCompletion(true);

        System.exit(0);
    }
}
