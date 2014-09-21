package ml.grafos.okapi.graphs;

import java.io.IOException;

import ml.grafos.okapi.common.data.LongArrayListWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a set of computation classes used to compute metrics related to 
 * triangles, for instance, the counting number of unique triangles in a graph.
 * 
 * For each metric, we have a different MasterCompute implementation that 
 * orchestrates the comptuation classes depending on which metric we want to
 * compute. Currently, we provide the following algorithms:
 * 
 * 1) Counting unique triangles
 * 2) Finding all the unique triangles
 * 
 * @author dl
 */
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
   * This computation initializes the values of every vertex to NullWritable.
   * 
   * @author dl
   *
   */
  public static class Initialize extends AbstractComputation<LongWritable, 
  NullWritable, NullWritable, Writable, Writable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable, NullWritable> vertex,
        Iterable<Writable> messages) throws IOException {
      vertex.setValue(NullWritable.get());
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
   * This class implements the second phase of the unique triangle counting 
   * algorithm. It forwards a received message, containing the ID of neighboring
   * vertices with lower IDs to all vertices that have higher ID than the vertex
   * that received the message.
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
        assert(msg.compareTo(vertex.getId())<0); // This can never happen
        for (Edge<WritableComparable, Writable> edge: vertex.getEdges()) {
          if (vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {
            sendMessage(edge.getTargetVertexId(), msg);
          }
        } 
      }
      vertex.voteToHalt();
    }
  }
 
  /**
   * This class detects whether a triangle has closed after a cycle of 
   * (i) propagating and (ii) forwarding vertex IDs. At this point, if the 
   * current vertex receives a message with an ID that corresponds to one of its
   * neighbors, it means that the current vertex participates in a triangle with
   * the vertex with the specific ID. For each such message, the current vertex
   * increases a counter. The final value of the counter is the number of 
   * triangles in which the current vertex has the maximum ID.
   * 
   * IMPORTANT: For efficiency, it is better to configure the job to use an
   * OutEdge class implementation that implements the StrictRandomAccessOutEdges
   * interface, such as the HashMapEdges. This is because, for  every message 
   * received, it checks for the existence of an edge with a specific ID. 
   * Other implementations would require to iterate over all edges for every 
   * message.
   * 
   * @author dl
   *
   */
  public static class CloseTrianglesAndCount extends 
  AbstractComputation<WritableComparable, Writable, Writable, 
  WritableComparable, WritableComparable> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<WritableComparable> messages) throws IOException {
      int count = 0;
      for (WritableComparable msg : messages) {
        // If this vertex has a neighbor with this ID, then this means it
        // participates in a triangle.
        if (vertex.getEdgeValue(msg)!=null) {
          count++;
        }
      }
      if (count>0) {
        vertex.setValue(new IntWritable(count));
      }
      vertex.voteToHalt();
    }
  }
  
  
  /**
   * This class implements the second phase of the algorithm tha finds all
   * unique triangles (not just counting) them. The difference with the 
   * ForwardId implementation, is that it sends a pair of IDs: the ID included
   * in the message sent from the first phase, and the ID of the current vertex.
   * 
   * @author dl
   *
   */
  public static class ForwardIdAndSource extends 
  AbstractComputation<WritableComparable, Writable, Writable, 
  WritableComparable, ArrayListWritable<WritableComparable>> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<WritableComparable> messages) throws IOException {
      for (WritableComparable msg : messages) {
        assert(msg.compareTo(vertex.getId())<0); // This can never happen
        for (Edge<WritableComparable, Writable> edge: vertex.getEdges()) {
          if (vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {

            final Vertex<WritableComparable, Writable, Writable> _vertex = vertex;

            ArrayListWritable<WritableComparable> idSrcPair = 
                new ArrayListWritable() {
              @Override
              public void setClass() {
                setClass(_vertex.getId().getClass());
              }
            };

            idSrcPair.add(msg);
            idSrcPair.add(vertex.getId());
            sendMessage(edge.getTargetVertexId(), idSrcPair);
          }
        } 
      }
      vertex.voteToHalt();
    }
  }

  /**
   * This class implements the third phase of the algorithm that finds all the
   * unique triangles (not just counting).
   * 
   * @author dl
   *
   */
  public static class FindTriangles extends 
    AbstractComputation<WritableComparable, Writable, Writable, 
    ArrayListWritable<WritableComparable>, WritableComparable> {

    @Override
    public void compute(Vertex<WritableComparable, Writable, Writable> vertex, 
        Iterable<ArrayListWritable<WritableComparable>> messages) 
            throws IOException {

      ArrayListWritable<ArrayListWritable<WritableComparable>> triangles = 
          new ArrayListWritable() {
            @Override
            public void setClass() {
              setClass(ArrayListWritable.class);
            }
      };

      for (ArrayListWritable<WritableComparable> msg : messages) {
        // If this vertex has a neighbor with this ID, then this means it
        // participates in a triangle.
        if (vertex.getEdgeValue(msg.get(0))!=null) {
          final Vertex<WritableComparable, Writable, Writable> _vertex = vertex;
          ArrayListWritable<WritableComparable> pair = new ArrayListWritable() {
            @Override
            public void setClass() {
              setClass(_vertex.getId().getClass());
            }
          };
          pair.add(msg.get(0));
          pair.add(msg.get(1));
          triangles.add(pair);
        }
      }
      if (triangles.size()>0) {
        vertex.setValue(triangles);
      }
      vertex.voteToHalt();
    }
  }

  
  /**
   * Use this MasterCompute implementation to count the number of unique 
   * triangles.
   * 
   * @author dl
   *
   */
  public static class TriangleCount extends DefaultMasterCompute {
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
      } else {
        setComputation(CloseTrianglesAndCount.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } 
    }
  }
  

  /**
   * Use this MasterCompute implementation to find the actual unique triangles.
   * 
   * @author dl
   *
   */
  public static class TriangleFind extends DefaultMasterCompute {
    
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
        setComputation(ForwardIdAndSource.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongArrayListWritable.class);
      } else {
        setComputation(FindTriangles.class);
        setIncomingMessage(LongArrayListWritable.class);
        setOutgoingMessage(LongWritable.class);
      } 
    }
  }
  

  /**
   * This class is similar to the {@link IdWithValueTextVertexOutputFormat}, only
   * if the value of a vertex is null or of type NullWritable, it passes to the
   * underlying LineRecordWriter a (null,null) key-value pair. The result of this
   * is to omit from the output those vertices with a null value.
   * 
   * We use this output format so that vertices that do not belong in a triangle
   * and, therefore, will have a null value at the end of the algorithm 
   * execution, do not get printed in the output.
   * 
   * @author dl
   *
   * @param <I>
   * @param <V>
   * @param <E>
   */
  public static class TriangleOutputFormat<I extends WritableComparable,
  V extends Writable, E extends Writable>
  extends TextVertexOutputFormat<I, V, E> {
    /** Split delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default split delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT =
        AdjacencyListTextVertexInputFormat.LINE_TOKENIZE_VALUE_DEFAULT;

    @Override
    public TriangleTextVertexWriter createVertexWriter(
        TaskAttemptContext context) {
      return new TriangleTextVertexWriter();
    }

    /**
     * Vertex writer associated with {@link TriangleOutputFormat}.
     */
    protected class TriangleTextVertexWriter extends
    TextVertexWriterToEachLine {
      /** Cached split delimeter */
      private String delimiter;

      @Override
      public void initialize(TaskAttemptContext context) throws IOException,
      InterruptedException {
        super.initialize(context);
        delimiter =
            getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      @Override
      public Text convertVertexToLine(Vertex<I, V, E> vertex)
          throws IOException {
        
        if (vertex.getValue()==null || 
            vertex.getValue() instanceof NullWritable) {
          return null;
        }

        StringBuffer sb = new StringBuffer(vertex.getId().toString());
        sb.append(delimiter);
        sb.append(vertex.getValue());

        return new Text(sb.toString());
      }
    }
  }
}
