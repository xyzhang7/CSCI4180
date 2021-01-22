import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class PRPreProcess {
    public static class PRPreMapper extends Mapper<Object, Text, Text, Text>{
        private Stack<String> nodeSet;
        @Override
        public void setup(Context context){
            nodeSet = new Stack<String>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());

            if(itr.hasMoreTokens()){
                String src = itr.nextToken();
                String des = itr.nextToken();

                //If des is a dangling point
                if(!nodeSet.contains(des)){
                    context.write(new Text(des), new Text("-1"));
                }

                nodeSet.add(src);
                nodeSet.add(des);

                context.write(new Text(src), new Text(des));
            }
        }
    }


    public static class PRPreReducer extends Reducer<Text, Text, Text, PRNodeWritable>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Integer nodeId = Integer.parseInt(key.toString());
            PRNodeWritable node = new PRNodeWritable(nodeId, 1, true);
            for(Text val : values){
                int m = Integer.parseInt(val.toString());
                if(m != -1){
                    node.add(m);
                }
            }
//            System.out.println(node.toString());
            context.getCounter(PageRank.COUNTER.NODENUM).increment(1);
            context.write(key, node);
        }
    }

    public static Job preConfig(Configuration conf, String inPath, String outPath)throws Exception{

        Job job = Job.getInstance(conf, "PRPreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreMapper.class);
        job.setReducerClass(PRPreReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }
}



















