package ml.grafos.okapi.graphs;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Triangles  {

  /**
   * This class is the computation class for superstep 0 and is used only to set
   * the types of I,V,E, so that the rest of the classes can be generic. 
   * It is a NO-OP.
   * 
   * Using it does cause an unnecessary overhead of iterating over all vertices
   * in the graph, but this cost should be insignificant relative to the cost
   * of the main algorithm.
   * 
   * @author dl
   *
   */
  public static class Initialize extends AbstractComputation<LongWritable, 
  IntWritable, NullWritable, Writable, Writable> {

    @Override
    public void compute(Vertex<LongWritable, IntWritable, NullWritable> vertex,
        Iterable<Writable> messages) throws IOException {
    }
  }

  /**
   * This class implements the first stage, which propagates the ID of a vertex
   * to all neighbors with higher ID.
   * 
   * We assume that 
   * 
   * @author dl
   *
   */
  public static class PropagateId extends AbstractComputation<WritableComparable, 
  Writable, Writable, Writable, Writable> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<Writable> messages) throws IOException {
      for (Edge<WritableComparable, Writable> edge: vertex.getEdges()) {
        if (edge.getTargetVertexId().compareTo(vertex.getId()) > 0) {
          sendMessage(edge.getTargetVertexId(), vertex.getId());
        }
      }
      vertex.voteToHalt();
    }
  } 

  /**
   * This class implements the second phase, which forwards a received message
   * to all vertices that have higher ID than the vertex that received the
   * message.
   * 
   * @author dl
   *
   */
  public static class ForwardId extends AbstractComputation<WritableComparable, 
  Writable, Writable, WritableComparable, WritableComparable> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<WritableComparable> messages) throws IOException {
      for (WritableComparable msg : messages) {
        for (Edge<WritableComparable, Writable> edge: vertex.getEdges()) {
          if (msg.compareTo(vertex.getId()) < 0 &&
              vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {
            sendMessage(edge.getTargetVertexId(), msg);
          }
        } 
      }
      vertex.voteToHalt();
    }
  }

  public static class CloseTriangles extends AbstractComputation<WritableComparable, 
  Writable, Writable, WritableComparable, WritableComparable> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<WritableComparable> messages) throws IOException {
      
      for (WritableComparable msg : messages) {
        // If this vertex has a neighbor with this ID, then forward the message
        // to it.
        for (Edge<WritableComparable, Writable> edge : vertex.getEdges()) {
          if (edge.getTargetVertexId().compareTo(msg) == 0) {
            sendMessage(edge.getTargetVertexId(), msg);
          }
        }
      }
      vertex.voteToHalt();
    }
  }
  
  public static class Count extends AbstractComputation<WritableComparable, 
  IntWritable, Writable, WritableComparable, WritableComparable> {

    @Override
    public void compute(Vertex<WritableComparable, IntWritable, Writable> vertex,
        Iterable<WritableComparable> messages) throws IOException {
      int count = 0;
      for (WritableComparable msg : messages) {
        count++;
      }
      vertex.setValue(new IntWritable(count));
      vertex.voteToHalt();
    }
  }
  
  public static class MasterCompute extends DefaultMasterCompute {
    @Override
    public void compute() {
      long superstep = getSuperstep();  
      if (superstep==0) {
        setComputation(Initialize.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } else if (superstep==1) {
        setComputation(PropagateId.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } else if (superstep==2) {
        setComputation(ForwardId.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } else if (superstep==3) {
        setComputation(CloseTriangles.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } else {
        setComputation(Count.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      }
    }
  }
}
