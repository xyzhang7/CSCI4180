import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PRNodeWritable implements Writable{
    private int nodeId;
    private double p;
    private List<Integer> adjList;
    private boolean flag; //true: a node; false: not a node

    public PRNodeWritable(){

    }

    public PRNodeWritable(int nodeId, double p, boolean flag){
        this.nodeId = nodeId;
        this.p = p;
        this.flag = flag;
        this.adjList = new ArrayList<Integer>();
    }

    public PRNodeWritable(String value){
        String[] tokens = value.split("\\s+");
        this.nodeId = Integer.parseInt(tokens[1]);
        this.p = Double.parseDouble(tokens[2]);
        this.flag = Boolean.parseBoolean(tokens[3]);
        this.adjList = new ArrayList<Integer>();

        for(int i=4; i<tokens.length; i++){
            add(Integer.parseInt(tokens[i]));
        }
    }

    public void add(int m){
        this.adjList.add(m);
    }

    public void set(PRNodeWritable node){
        this.nodeId = node.getNodeId();
        this.p = node.getP();
        this.flag = node.isFlag();
        this.adjList = new ArrayList<Integer>();
        for(Integer val: node.adjList){
            add(val);
        }
    }

    public void setP(double p) {
        this.p = p;
    }

    public int getNodeId() {
        return nodeId;
    }

    public double getP() {
        return p;
    }

    public List<Integer> getAdjList() {
        return adjList;
    }

    public boolean isFlag() {
        return flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.nodeId);
        dataOutput.writeDouble(this.p);
        dataOutput.writeBoolean(this.flag);
        dataOutput.writeInt(this.adjList.size());
        for(Integer val: adjList){
            dataOutput.writeInt(val);
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.nodeId = dataInput.readInt();
        this.p = dataInput.readDouble();
        this.flag = dataInput.readBoolean();
        int adjListSize = dataInput.readInt();
        this.adjList = new ArrayList<Integer>();
        for(int i=0; i<adjListSize; i++){
            int val = dataInput.readInt();
            add(val);
        }
    }

    public String toString(){
        StringBuilder s = new StringBuilder();
        s.append(nodeId).append(" ").append(p).append(" ").append(flag).append(" ");
        if(adjList != null){
            for(Integer val : adjList){
                s.append(val.toString()).append(" ");
            }
        }
        return s.toString();
    }
}
