package ml.grafos.okapi.graphs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

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
  /**
   * Property name for the total trust.
   */
  public static final String TOTAL_TRUST = "sybilrank.total.trust";
  
  /**
   * Property name for the iteration multiplier.
   */
  public static final String ITERATION_MULTIPLIER = 
      "sybilrank.iteration.multiplier";
  
  /**
   * Default multiplier for the iterations.
   */
  public static final int ITERATION_MULTIPLIER_DEFAULT = 1;
  
  /**
   * Name of aggregator used to calculate the total number of trusted nodes.
   */
  public static final String AGGREGATOR_NUM_TRUSTED = "AGG_NUM_TRUSTED";
  
  public static final LongWritable ONE = new LongWritable(1);

  /**
   * This method computes the degree of a vertex as the sum of its edge weights.
   * @param v
   * @return
   */
  public static double computeDegree(
      Vertex<LongWritable,VertexValue,DoubleWritable> v) {
    double degree = 0.0;
    for (Edge<LongWritable, DoubleWritable> edge : v.getEdges()) {
      degree += edge.getValue().get();
    }    
    return degree;
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
  extends AbstractComputation<LongWritable, VertexValue, DoubleWritable, 
  DoubleWritable, DoubleWritable> {

    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
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
  DoubleWritable, DoubleWritable> {

    private double totalTrust; 
    
    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, DoubleWritable> vertex,
        Iterable<DoubleWritable> messages) throws IOException {
      
      if (vertex.getValue().isTrusted()) {
        vertex.getValue().setRank(
            totalTrust/(double)((LongWritable)getAggregatedValue(
                AGGREGATOR_NUM_TRUSTED)).get());
      } else {
        vertex.getValue().setRank(0.0);
      }
      
      double degree = computeDegree(vertex);
      
      // Distribute rank to edges proportionally to the edge weights
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double distRank = 
            vertex.getValue().getRank()*(edge.getValue().get()/degree);
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distRank));
      }
    }
    
    @Override
    public void preSuperstep() {
      String s_totalTrust = getContext().getConfiguration().get(TOTAL_TRUST); 
      if (s_totalTrust != null) {
        totalTrust = Double.parseDouble(s_totalTrust);
      } else {
        // The default value of the total trust is equal to the number of
        // vertices in the graph.
        totalTrust = getTotalNumVertices();
      }
    }
  }
  
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
      double newRank = 0.0;
      for (DoubleWritable message : messages) {
        newRank += message.get();
      }
      
      double degree = computeDegree(vertex);
      
      // Distribute rank to edges proportionally to the edge weights
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double distRank = newRank*(edge.getValue().get()/degree);
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distRank));
      }
      
      // The final value of the rank is normalized by the degree of the vertex.
      vertex.getValue().setRank(newRank/degree);
    }
  }
  
  /**
   * This implementation coordinates the execution of the SybilRank algorithm.
   * 
   * @author dl
   *
   */
  public static class SybilRankMasterCompute extends DefaultMasterCompute {
    private int iterationMultiplier;

    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      
      iterationMultiplier = getContext().getConfiguration().getInt(
          ITERATION_MULTIPLIER, ITERATION_MULTIPLIER_DEFAULT);       
      
      // Register the aggregator that will be used to count the number of 
      // trusted nodes.
      registerPersistentAggregator(AGGREGATOR_NUM_TRUSTED,
          LongSumAggregator.class);
    }

    @Override
    public void compute() {
      long superstep = getSuperstep();
      if (superstep == 0) {
        setComputation(TrustAggregation.class);
      } else if (superstep == 1) {
        setComputation(Initializer.class);
      } else {
        setComputation(SybilRankComputation.class);
      }
      
      // The number of power iterations we execute is equal to c*log10(N), where
      // N is the number of vertices in the graph and c is the iteration
      // multiplier.
      if (superstep>0) {
        int maxPowerIterations = (int)Math.ceil(
            iterationMultiplier*Math.log10((double)getTotalNumVertices()));
        // Before the power iterations, we execute 2 initial supersteps, so we 
        // count those in when deciding to stop. 
        if (superstep >= 2+maxPowerIterations) {
          haltComputation();
        }
      }
    }
  }

  /**
   * Represents the state of a vertex for this algorithm. This state indicates
   * the current rank of the vertex and whether this vertex is considered
   * trusted or not.
   * 
   * Unless explicitly set, a vertex is initialized to be untrusted.
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
      isTrusted = false;
    }

    public VertexValue(double rank, boolean isTrusted) {
      this.rank = rank;
      this.isTrusted = isTrusted;
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
    
    @Override
    public String toString() {
      return String.valueOf(rank);
    }
  }

  /**
   * This InputFormat class is used to read the set of vertices that are 
   * considered trusted. The actual input is expected to contain one vertex
   * ID per line.
   * @author dl
   *
   */
  public static class SybilRankVertexValueInputFormat extends
  TextVertexValueInputFormat<LongWritable, VertexValue, DoubleWritable> {

    @Override
    public SybilRankVertexValueReader createVertexValueReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new SybilRankVertexValueReader();
    }

    public class SybilRankVertexValueReader extends
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
        value.setTrusted(true);
        return value;
      }
    }
  }
}
