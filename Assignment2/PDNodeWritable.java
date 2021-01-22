import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.util.Set;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class PDNodeWritable implements Writable{
    private IntWritable distance;
    private Text incomingNode;
    private MapWritable edgeList;

    public PDNodeWritable(){
        distance = new IntWritable();
        incomingNode = new Text();
        edgeList = new MapWritable();
    }

    public PDNodeWritable(IntWritable distance, Text incomingNode){
        this.distance = distance;
        this.setIncomingNode(incomingNode);
        edgeList = new MapWritable();
    }

    public IntWritable getDistance() {
        return distance;
    }

    public void setDistance(IntWritable distance) {
        this.distance = distance;
    }

    public MapWritable getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(MapWritable edgeList) {
        this.edgeList = edgeList;
    }

    public Text getIncomingNode() {
        return incomingNode;
    }

    public void setIncomingNode(Text incomingNode) {
        this.incomingNode = incomingNode;
    }

    public void readFields(DataInput in) throws IOException {
        distance.readFields(in);
        incomingNode.readFields(in);
        edgeList.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        distance.write(out);
        incomingNode.write(out);
        edgeList.write(out);
    }

    public String toString(){
        StringBuilder output = new StringBuilder();
        Set<Writable> keys = edgeList.keySet();
        for (Writable key : keys) {
            output.append(((Text)key).toString() + " ");
            output.append(((Text)edgeList.get(key)).toString() + " ");
        }
        return distance.toString() + " " + incomingNode.toString() + " " + output.toString()  ;
    }
}
