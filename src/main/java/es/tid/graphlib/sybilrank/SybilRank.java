package es.tid.graphlib.sybilrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.tid.graphlib.common.PropagateId;
import es.tid.graphlib.common.ConverterUpdateEdges;

/**
 * This is an implementation of the SybilRank algorithm. In fact, this is an
 * extension of the original SybilRank algorithm published by Cao et al at
 * NSDI'12. This version of the algorithm assumes a weighted graph. The modified
 * algorithm has been developed by Boshmaf et al.
 * 
 * @author dl
 *
 */
public class SybilRank {
  
  public static final String EDGE_WEIGHT = "socc.weight";
  public static final byte DEFAULT_EDGE_WEIGHT = 2;
  
  /**
   * Property name for the total trust.
   */
  public static final String TOTAL_TRUST = "sybilrank.total.trust";
  
  /**
   * Default total trust.
   */
  public static final Double TOTAL_TRUST_DEFAULT = 1.0;
  
  /**
   * Name of aggregator used to calculate the total number of trusted nodes.
   */
  public static final String AGGREGATOR_NUM_TRUSTED = "AGG_NUM_TRUSTED";
  
  // Final value of 1L used by the aggregators.
  public static final LongWritable ONE = new LongWritable(1);

  /**
   * This class implements the main part of the SybilRank algorithms, that is,
   * the power iterations.
   * 
   * @author dl
   *
   */
  public static class SybilRankComputation
  extends AbstractComputation<LongWritable, VertexValue, DoubleWritable, 
  DoubleWritable, DoubleWritable> {
    
    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      
      // Aggregate rank from friends.
      double newRank = 0;
      for (DoubleWritable message : messages) {
        newRank += message.get();
      }
      
      // This is the new rank of this vertex.
      vertex.getValue().setRank(newRank);
      
      // Distribute rank to edges proportionally to the edge weights
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double distRank = vertex.getValue().getRank()*
            (edge.getValue().get()/(double)vertex.getNumEdges());
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distRank));
      }
    }
  }


  /**
   * This computation class is used to calculate the aggregate number of
   * trusted nodes. This value is necessary to initialize the rank of the nodes
   * before the power iterations starts.
   * 
   * @author dl
   *
   */
  public static class TrustAggregation 
  extends AbstractComputation<WritableComparable, VertexValue, Writable, 
  Writable, Writable> {

    @Override
    public void compute(
        Vertex<WritableComparable, VertexValue, Writable> vertex,
        Iterable<Writable> messages) throws IOException {
      if (vertex.getValue().isTrusted()) {
        aggregate(AGGREGATOR_NUM_TRUSTED, ONE);
      }
    }
  }

  /**
   * This class is used only to initialize the rank of the vertices. It assumes
   * that the trust aggregation computations has occurred in the previous step.
   * 
   * After the initialization it also distributes the rank of every vertex to
   * it friends, so that the power iterations start. 
   * 
   * @author dl
   *
   */
  public static class Initializer 
  extends AbstractComputation<LongWritable, VertexValue, DoubleWritable, 
  Writable, DoubleWritable> {

    private double totalTrust; 
    
    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
        Iterable<Writable> messages) throws IOException {
      
      if (vertex.getValue().isTrusted()) {
        vertex.getValue().setRank(
            totalTrust/(double)((LongWritable)getAggregatedValue(
                AGGREGATOR_NUM_TRUSTED)).get());
      } else {
        vertex.getValue().setRank(0.0);
      }
      
      // Distribute rank to edges proportionally to the edge weights
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double distRank = vertex.getValue().getRank()*
            (edge.getValue().get()/(double)vertex.getNumEdges());
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distRank));
      }
    }
    
    @Override
    public void preSuperstep() {
      String s_totalTrust = getContext().getConfiguration().get(TOTAL_TRUST); 
      if (s_totalTrust != null) {
        totalTrust = Double.parseDouble(s_totalTrust);
      } else {
        totalTrust = TOTAL_TRUST_DEFAULT;
      }
    }
  }
  
  
  /**
   * This implementation coordinates the execution of the SybilRank algorithm.
   * 
   * @author dl
   *
   */
  public static class SybilRankMasterCompute extends DefaultMasterCompute {
    
    private int maxPowerIterations;

    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
            
      // Register the aggregator that will be used to count the number of 
      // trusted nodes.
      registerPersistentAggregator(AGGREGATOR_NUM_TRUSTED,
          LongSumAggregator.class);
      
      // The number of power iterations we execute is equal to log10(N), where
      // N is the number of vertices in the graph.
      maxPowerIterations = (int)Math.log10((double)getTotalNumVertices());
    }

    @Override
    public void compute() {
      int superstep = (int) getSuperstep();
      if (superstep == 0) {
        setComputation(PropagateId.class);
      } else if (superstep == 1) {
        setComputation(ConverterUpdateEdges.class);
      } else if (superstep == 2) {
        setComputation(TrustAggregation.class);
      } else if (superstep == 3) {
        setComputation(Initializer.class);
      } else {
        setComputation(SybilRankComputation.class);
      }
      
      // Before the power iterations, we execute 4 initial supersteps, so we 
      // count those in when deciding to stop. 
      if (4+superstep > maxPowerIterations) {
        haltComputation();
      }
    }
  }

  /**
   * Represents the state of a vertex for this algorithm. This state indicates
   * the current rank of the vertex and whether this vertex is considered
   * trusted or not.
   * 
   * Unless explicitly set, a vertex is initialized to be trusted.
   * 
   * @author dl
   *
   */
  public static class VertexValue implements Writable {
    // Indicates whether this vertex is considered trusted.
    private boolean isTrusted;
    // This holds the current rank of the vertex.
    private double rank;

    public VertexValue() {
      isTrusted = true;
    }

    public void setRank(double rank) {
      this.rank = rank;
    }
    
    public double getRank() {
      return rank;
    }
    
    public void setTrusted(boolean isTrusted) {
      this.isTrusted = isTrusted;
    }
    
    public boolean isTrusted() {
      return isTrusted;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      rank = in.readDouble();
      isTrusted = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(rank);
      out.writeBoolean(isTrusted);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      VertexValue that = (VertexValue) o;
      if (rank != that.rank ||
          isTrusted != that.isTrusted) {
        return false;
      }
      return true;
    }
  }

  /**
   * This InputFormat class is used to read the set of vertices that are 
   * considered untrusted. The actual input is expected to contain one vertex
   * ID per line.
   * @author dl
   *
   */
  public static class SybilRankVertexValueInputFormat extends
  TextVertexValueInputFormat<LongWritable, VertexValue, Writable> {

    @Override
    public LongShortTextVertexValueReader createVertexValueReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new LongShortTextVertexValueReader();
    }

    public class LongShortTextVertexValueReader extends
    TextVertexValueReaderFromEachLineProcessed<String> {

      @Override
      protected String preprocessLine(Text line) throws IOException {
        return line.toString();
      }

      @Override
      protected LongWritable getId(String data) throws IOException {
        return new LongWritable(Long.parseLong(data));
      }

      @Override
      protected VertexValue getValue(String data) throws IOException {
        VertexValue value = new VertexValue();
        value.setTrusted(false);
        return value;
      }
    }
  }

  /**
   * This input format is used to read the graph structure. It assumes a set
   * of weighted edges in the form of:
   * 
   * <src id> <dst id> <edge weight>
   * ...
   * 
   * The vertex IDs are expected to be of type long and the weights are
   * expected to be of type double.
   * 
   * @author dl
   *
   */
  public static class LongDoubleTextEdgeInputFormat extends
  TextEdgeInputFormat<LongWritable, DoubleWritable> {
    /** Splitter for endpoints */
    private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

    @Override
    public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new LongDoubleTextEdgeReader();
    }

    public class LongDoubleTextEdgeReader extends
    TextEdgeReaderFromEachLineProcessed<String[]> {
      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        return SEPARATOR.split(line.toString());
      }

      @Override
      protected LongWritable getSourceVertexId(String[] tokens)
          throws IOException {
        return new LongWritable(Long.parseLong(tokens[0]));
      }

      @Override
      protected LongWritable getTargetVertexId(String[] tokens)
          throws IOException {
        return new LongWritable(Long.parseLong(tokens[1]));
      }

      @Override
      protected DoubleWritable getValue(String[] tokens) throws IOException {
        return new DoubleWritable(Double.parseDouble(tokens[2]));
      }
    }
  }
  
}
