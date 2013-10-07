package es.tid.graphlib.sybilrank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import es.tid.graphlib.common.PropagateId;
import es.tid.graphlib.common.ConverterUpdateEdges;

/**
 * This is an implementation of the SybilRank algorithm. In fact, this is an
 * extension of the original SybilRank algorithm published by Cao et al at
 * NSDI'12 that assumes a weighted graph. The modified algorithm has been 
 * developed by Boshmaf et al.
 * 
 * @author dl
 *
 */
public class SybilRank {
  private static final String MAX_ITERATIONS = "socc.maxIterations";
  private static final int DEFAULT_MAX_ITERATIONS = 290;
  public static final String EDGE_WEIGHT = "socc.weight";
  public static final byte DEFAULT_EDGE_WEIGHT = 2;
  public static final String AGGREGATOR_NUM_TRUSTED = "AGG_NUM_TRUSTED";
  public static final LongWritable ONE = new LongWritable(1);

  public static class ComputeNewPartition
  extends AbstractComputation<LongWritable, VertexValue, EdgeValue, PartitionMessage, NullWritable> {
    private List<Short> maxIndices = Lists.newArrayList();
    private Random rnd = new Random();
    private String[] demandAggregatorNames;
    private int[] partitionFrequency;
    private long[] loads;
    private long totalCapacity;
    private short numberOfPartitions;
    private double additionalCapacity;
    private double lambda;

    private double computeW(int newPartition) {
      //return weights[newPartition];
      return new BigDecimal(((double) loads[newPartition]) / totalCapacity).setScale(3, BigDecimal.ROUND_CEILING).doubleValue();
    }

    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        Iterable<PartitionMessage> messages) throws IOException {
      boolean isActive = messages.iterator().hasNext();
      short currentPartition  = vertex.getValue().getCurrentPartition();
      int numberOfEdges = vertex.getNumEdges();

      // update neighbors partitions
      updateNeighborsPartitions(vertex, messages);

      // count labels occurrences in the neighborhood
      int totalLabels = computeNeighborsLabels(vertex);

      // compute the most attractive partition
      short newPartition = computeNewPartition(vertex, totalLabels);

      // request migration to the new destination
      if (newPartition != currentPartition && isActive) {
        requestMigration(vertex, numberOfEdges, currentPartition, newPartition);
      }

      //IntermediateResultsPrinterWorkerContext wc = (IntermediateResultsPrinterWorkerContext) getWorkerContext();
      //wc.writeVertex(vertex);
    }

    @Override
    public void preSuperstep() {
      additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY, DEFAULT_ADDITIONAL_CAPACITY);
      numberOfPartitions = (short) getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
      lambda = getContext().getConfiguration().getFloat(LAMBDA, DEFAULT_LAMBDA);
      partitionFrequency = new int[numberOfPartitions];
      loads = new long[numberOfPartitions];
      demandAggregatorNames = new String[numberOfPartitions];
      totalCapacity = (long) Math.round(((double) getTotalNumEdges() * (1 + additionalCapacity) / numberOfPartitions));
      // cache loads for the penalty function
      for (int i = 0; i < numberOfPartitions; i++) {
        demandAggregatorNames[i] = AGGREGATOR_DEMAND_PREFIX + i;
        loads[i] = ((LongWritable) getAggregatedValue(AGGREGATOR_LOAD_PREFIX + i)).get();
      }
      //System.out.println("weights " + Arrays.toString(loads));
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
  extends AbstractComputation<LongWritable, VertexValue, EdgeValue, Writable, 
  Writable> {

    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        Iterable<Writable> messages) throws IOException {
      if (vertex.getValue().isTrusted()) {
        aggregate(AGGREGATOR_NUM_TRUSTED, ONE);
      }
    }
  }

  public static class SybilRankMasterCompute
  extends DefaultMasterCompute {
    private String[] loadAggregatorNames;
    private int maxIterations;
    private int numberOfPartitions;
    private double previousState;
    private double convergenceThreshold;

    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS, DEFAULT_MAX_ITERATIONS);
      registerPersistentAggregator(AGGREGATOR_NUM_TRUSTED,
          LongSumAggregator.class);
    }

    private void printStats(int superstep) {
      System.out.println("superstep " + superstep);
      
      /*
      long migrations = ((LongWritable) getAggregatedValue(AGGREGATOR_MIGRATIONS)).get();
      long localEdges = ((LongWritable) getAggregatedValue(AGGREGATOR_LOCALS)).get();
      if (superstep > 2) {
        switch (superstep % 2) {
        case 0:
          System.out.println(((double) localEdges) / getTotalNumEdges() + " local edges");
          long minLoad = Long.MAX_VALUE;
          long maxLoad = - Long.MAX_VALUE;
          for (int i = 0; i < numberOfPartitions; i++) {
            long load = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
            if (load < minLoad) {
              minLoad = load;
            }
            if (load > maxLoad) {
              maxLoad = load;
            }
          }
          double expectedLoad = ((double) getTotalNumEdges()) / numberOfPartitions;
          System.out.println((((double) maxLoad) / minLoad) + " max-min unbalance");
          System.out.println((((double) maxLoad) / expectedLoad) + " maximum normalized load");
          break;
        case 1:
          System.out.println(migrations + " migrations");
          break;
        }
      }
      */
    }

    private boolean algorithmConverged(int superstep) {
      double newState = ((DoubleWritable) getAggregatedValue(AGGREGATOR_STATE)).get();
      boolean converged = false;
      if (superstep > 5) {
        double step = Math.abs(1 - newState / previousState);
        converged = step < convergenceThreshold;
        System.out.println("PreviousState=" + previousState + " NewState=" + newState + " " + step);
      }
      previousState = newState;
      return converged;
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
      } else {
        switch (superstep % 2) {
        case 0:
          setComputation(ComputeMigration.class);
          break;
        case 1:
          setComputation(ComputeNewPartition.class);
          break;
        }
      }
      boolean hasConverged = false;
      if (superstep > 3) {
        if (superstep % 2 == 0) {
          hasConverged = algorithmConverged(superstep);
        }
      }
      printStats(superstep);
      if (hasConverged || superstep >= maxIterations) {
        System.out.println("Halting computation: " + hasConverged);
        haltComputation();
      }
    }
  }

  public static class EdgeValue implements Writable {
    private short partition = -1;
    //private byte weight = 1;

    public EdgeValue() { }

    public EdgeValue(short partition, byte weight) {
      setPartition(partition);
      setWeight(weight);
    }

    public short getPartition() {
      return partition;
    }

    public void setPartition(short partition) {
      this.partition = partition;
    }

    public byte getWeight() {
      //return weight;
      return 1;
    }

    public void setWeight(byte weight) {
      //this.weight = weight;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      partition = in.readShort();
      //weight = in.readByte();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(partition);
      //out.writeByte(weight);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EdgeValue that = (EdgeValue) o;
      return this.partition == that.partition;
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
  TextVertexValueInputFormat<LongWritable, VertexValue, EdgeValue> {

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

  public static class LongShortTextEdgeInputFormat extends
  TextEdgeInputFormat<LongWritable, EdgeValue> {
    /** Splitter for endpoints */
    private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

    @Override
    public EdgeReader<LongWritable, EdgeValue> createEdgeReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new LongShortTextEdgeReader();
    }

    public class LongShortTextEdgeReader extends
    TextEdgeReaderFromEachLineProcessed<String[]> {
      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        return SEPARATOR.split(line.toString());
      }

      @Override
      protected LongWritable getSourceVertexId(String[] endpoints)
          throws IOException {
        return new LongWritable(Long.parseLong(endpoints[0]));
      }

      @Override
      protected LongWritable getTargetVertexId(String[] endpoints)
          throws IOException {
        return new LongWritable(Long.parseLong(endpoints[1]));
      }

      @Override
      protected EdgeValue getValue(String[] endpoints) throws IOException {
        EdgeValue value = new EdgeValue();
        if (endpoints.length == 3) {
          value.setWeight((byte) Byte.parseByte(endpoints[2]));
        }
        return value;
      }
    }
  }
  
}
