import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.List;

public class PageRank {
    public enum COUNTER{
        NODENUM
    }

    public static class PRMapper extends Mapper<Object, Text, Text, PRNodeWritable>{

        private long nodeNum;
        private boolean isFirstIte;

        @Override
        public void setup(Context context) {
            isFirstIte = Boolean.parseBoolean(context.getConfiguration().get("is first iteration"));
//            System.out.println("Map is First Iteration: " + isFirstIte);
            nodeNum = Long.parseLong(context.getConfiguration().get("node number"));
//            System.out.println("Map nodeNum: " + nodeNum);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            PRNodeWritable node = new PRNodeWritable(value.toString());
            Integer nodeId = node.getNodeId();
            List<Integer> adjList = node.getAdjList();

            if(isFirstIte){
                node.setP(1.0 /nodeNum);
            }

            context.write(new Text(nodeId.toString()), node);

            if(adjList.size() !=0 ){
                double m_PR = node.getP() / adjList.size();

                for(Integer val : adjList){
                    PRNodeWritable M = new PRNodeWritable(val, m_PR,false);
                    context.write(new Text(val.toString()), M);
//                System.out.println("\tMap write node " + M.toString());
                }
            }else{
                PRNodeWritable M = new PRNodeWritable(nodeId, node.getP(), false);
                context.write(new Text(nodeId.toString()), M);
            }

        }
    }

    public static class PRReducer extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable>{
        public void reduce(Text key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException{
            PRNodeWritable node = new PRNodeWritable();
            double sumP = 0;
            for(PRNodeWritable val: values){
                if(val.isFlag()) {
                    node.set(val);
                } else{
                    sumP = sumP + val.getP();
                }
//                System.out.println("\tReduce Node " + val.toString());
//                System.out.println("\tReduce PR Node " + val.toString());
            }
            node.setP(sumP);
//            System.out.println("Reduce write node " + node.toString());
            context.write(key, node);
        }
    }

    public static class PRPostMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        private double threshold;

        @Override
        public void setup(Context context) {
            threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split("\\s+");
            int nodeId = Integer.parseInt(tokens[1]);
            double p = Double.parseDouble(tokens[2]);
            if(p >= threshold){
                context.write(new IntWritable(nodeId), new DoubleWritable(p));
            }
        }
    }

    public static class PRPostReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            for(DoubleWritable val: values){
                context.write(key, val);
            }
        }
    }

    public static Job PRConfig(Configuration conf, int i) throws IOException{
        Job job = Job.getInstance(conf, "iteration-" + i);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PRNodeWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        //Path: /hty/q2
        FileInputFormat.addInputPath(job, new Path("/hty/q2/tmp/iteration-"
        + (i-1) + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("/hty/q2/tmp/iteration-"
        + i));
        return job;
    }

    public static Job PostPRConfig(Configuration conf, int i, String outPath) throws  IOException{
        Job job = Job.getInstance(conf, "PostPR");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRPostMapper.class);
        job.setReducerClass(PRPostReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("/hty/q2/tmp/iteration-"
        + (i-1) + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job;
    }

    public static void main(String[] args) throws Exception{

        double alpha = Double.parseDouble(args[0]);
        int ite = Integer.parseInt(args[1]);
        double threshold = Double.parseDouble(args[2]);
        String inPath = args[3];
        String outPath = args[4];
        Configuration conf = new Configuration();

        /* Preprocess */
        // Path: /Users/zhangxinyu/Downloads/Github/MapReduceApp/tmp
        Job preJob = PRPreProcess.preConfig(conf, inPath, "/hty/q2/tmp/iteration-0");
        preJob.waitForCompletion(true);
        long nodeNum = preJob.getCounters().findCounter(COUNTER.NODENUM).getValue();

        /* Set Configuration of Page Rank */
        conf.setLong("node number", nodeNum);
        conf.set("threshold", args[2]);

        /* Page Rank Job Iteration */
//        System.out.println("The total number of nodes:" + nodeNum);
        int i = 0;
        if(ite > 0){
            for(i=1; i<=ite; i++){
//                System.out.println("Iteration " + i );
                conf.setBoolean("is first iteration", i == 1);
                Job job = PRConfig(conf, i);
                job.waitForCompletion(true);
            }
        }else{
            System.exit(0);
        }

        Job postJob = PostPRConfig(conf, i, outPath);
        postJob.waitForCompletion(true);
        System.exit(0);
    }
}
















