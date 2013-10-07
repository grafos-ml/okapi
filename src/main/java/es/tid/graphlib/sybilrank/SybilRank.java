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
  private static final String AGGREGATOR_LOAD_PREFIX = "AGG_LOAD_";
  private static final String AGGREGATOR_DEMAND_PREFIX = "AGG_DEMAND_";
  private static final String AGGREGATOR_STATE = "AGG_STATE";
  private static final String AGGREGATOR_MIGRATIONS = "AGG_MIGRATIONS";
  private static final String AGGREGATOR_LOCALS = "AGG_LOCALS";
  private static final String NUM_PARTITIONS = "socc.numberOfPartitions";
  private static final int DEFAULT_NUM_PARTITIONS = 32;
  private static final String ADDITIONAL_CAPACITY = "socc.additionalCapacity";
  private static final float DEFAULT_ADDITIONAL_CAPACITY = 0.05f;
  private static final String LAMBDA = "socc.lambda";
  private static final float DEFAULT_LAMBDA = 1.0f;
  private static final String MAX_ITERATIONS = "socc.maxIterations";
  private static final int DEFAULT_MAX_ITERATIONS = 290;
  private static final String CONVERGENCE_THRESHOLD = "socc.threshold";
  private static final float DEFAULT_CONVERGENCE_THRESHOLD = 0.001f;
  public static final String EDGE_WEIGHT = "socc.weight";
  public static final byte DEFAULT_EDGE_WEIGHT = 2;

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

    /*
     * Request migration to a new partition
     */
    private void requestMigration(Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        int numberOfEdges, short currentPartition, short newPartition) {
      vertex.getValue().setNewPartition(newPartition);
      aggregate(demandAggregatorNames[newPartition], new LongWritable(numberOfEdges));
      loads[newPartition]     += numberOfEdges;
      loads[currentPartition] -= numberOfEdges;
    }

    /*
     * Update the neighbor labels when they migrate
     */
    private void updateNeighborsPartitions(
        Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        Iterable<PartitionMessage> messages) {
      for (PartitionMessage message : messages) {
        LongWritable otherId = new LongWritable(message.getSourceId());
        EdgeValue oldValue = vertex.getEdgeValue(otherId);
        vertex.setEdgeValue(otherId, new EdgeValue(message.getPartition(), oldValue.getWeight()));
      }
    }

    /*
     * Compute the occurrences of the labels in the neighborhood
     */
    private int computeNeighborsLabels(Vertex<LongWritable, VertexValue, EdgeValue> vertex) {
      Arrays.fill(partitionFrequency, 0);
      int totalLabels = 0;
      int localEdges = 0;
      for (Edge<LongWritable, EdgeValue> e : vertex.getEdges()) {
        partitionFrequency[e.getValue().getPartition()] += e.getValue().getWeight();
        totalLabels += e.getValue().getWeight();
        if (e.getValue().getPartition() == vertex.getValue().getCurrentPartition()) {
          localEdges++;
        }
      }
      // update cut edges stats
      aggregate(AGGREGATOR_LOCALS, new LongWritable(localEdges));

      return totalLabels;
    }

    /*
     * Choose a random partition with preference to the current
     */
    private short chooseRandomPartitionOrCurrent(short currentPartition) {
      short newPartition;
      if (maxIndices.size() == 1) {
        newPartition = maxIndices.get(0);
      } else {
        // break ties randomly unless current
        if (maxIndices.contains(currentPartition)) {
          newPartition = currentPartition;
        } else {
          newPartition = maxIndices.get(rnd.nextInt(maxIndices.size()));
        }
      }
      return newPartition;
    }

    /*
     * Choose deterministically on the label with preference to the current
     */
    private short chooseMinLabelPartition(short currentPartition) {
      short newPartition;
      if (maxIndices.size() == 1) {
        newPartition = maxIndices.get(0);
      } else {
        if (maxIndices.contains(currentPartition)) {
          newPartition = currentPartition;
        } else {
          newPartition = maxIndices.get(0);
        }
      }
      return newPartition;
    }

    /*
     * Choose a random partition regardless
     */
    private short chooseRandomPartition() {
      short newPartition;
      if (maxIndices.size() == 1) {
        newPartition = maxIndices.get(0);
      } else {
        newPartition = maxIndices.get(rnd.nextInt(maxIndices.size()));
      }
      return newPartition;
    }

    /*
     *  Compute the new partition according to the neighborhood labels and the partitions' loads 
     */
    private short computeNewPartition(Vertex<LongWritable, VertexValue, EdgeValue> vertex, int totalLabels) {
      short currentPartition = vertex.getValue().getCurrentPartition();
      short newPartition = -1;
      double bestState = - Double.MAX_VALUE;
      double currentState = 0;
      maxIndices.clear();
      for (short i = 0; i < numberOfPartitions; i++) {
        // original LPA
        double LPA = ((double) partitionFrequency[i]) / totalLabels;
        // penalty function
        double PF  = lambda * computeW(i);  
        // compute the rank and make sure the result is > 0
        double H = lambda + LPA - PF;
        if (i == currentPartition) {
          currentState = H;
        } 
        if (H > bestState) {
          bestState = H;
          maxIndices.clear();
          maxIndices.add(i);
        } else if (H == bestState) {
          maxIndices.add(i);
        }
      }
      newPartition = chooseRandomPartitionOrCurrent(currentPartition);
      // update state stats
      aggregate(AGGREGATOR_STATE, new DoubleWritable(currentState));

      return newPartition;
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

  public static class ComputeMigration
  extends AbstractComputation<LongWritable, VertexValue, EdgeValue, NullWritable, PartitionMessage> {
    private Random rnd = new Random();
    private String[] loadAggregatorNames;
    private double[] migrationProbabilities;
    private short numberOfPartitions;
    private double additionalCapacity;

    private void migrate(Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        short currentPartition, short newPartition) {
      vertex.getValue().setCurrentPartition(newPartition);
      // update partitions loads
      int numberOfEdges = vertex.getNumEdges();
      aggregate(loadAggregatorNames[currentPartition], new LongWritable(- numberOfEdges));
      aggregate(loadAggregatorNames[newPartition], new LongWritable(numberOfEdges));
      aggregate(AGGREGATOR_MIGRATIONS, new LongWritable(1));
      // inform the neighbors
      PartitionMessage message = new PartitionMessage(vertex.getId().get(), newPartition);
      sendMessageToAllEdges(vertex, message);
    }

    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        Iterable<NullWritable> messages) throws IOException {
      if (messages.iterator().hasNext()) {
        throw new RuntimeException("messages in the migration step!");
      }
      short currentPartition = vertex.getValue().getCurrentPartition();
      short newPartition     = vertex.getValue().getNewPartition();
      if (currentPartition == newPartition) {
        return;
      }
      double migrationProbability = migrationProbabilities[newPartition];
      if (rnd.nextDouble() < migrationProbability) {
        migrate(vertex, currentPartition, newPartition);
      } else {
        vertex.getValue().setNewPartition(currentPartition);
      }
    }

    @Override
    public void preSuperstep() {
      additionalCapacity = getContext().getConfiguration().getFloat(ADDITIONAL_CAPACITY, DEFAULT_ADDITIONAL_CAPACITY);
      numberOfPartitions = (short) getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
      long totalCapacity = (long) Math.round(((double) getTotalNumEdges() * (1 + additionalCapacity) / numberOfPartitions));
      migrationProbabilities = new double[numberOfPartitions];
      loadAggregatorNames = new String[numberOfPartitions];
      // cache migration probabilities per destination partition
      for (int i = 0; i < numberOfPartitions; i++) {
        loadAggregatorNames[i] = AGGREGATOR_LOAD_PREFIX + i;
        long load   = ((LongWritable) getAggregatedValue(loadAggregatorNames[i])).get();
        long demand = ((LongWritable) getAggregatedValue(AGGREGATOR_DEMAND_PREFIX + i)).get();
        long remainingCapacity = totalCapacity - load;
        if (demand == 0 || remainingCapacity <= 0) {
          migrationProbabilities[i] = 0;
        } else {
          migrationProbabilities[i] = ((double) (remainingCapacity)) / demand;
        }
      }
      //System.out.println("migration probabilities: " + Arrays.toString(migrationProbabilities));
    }
  }

  public static class Initializer
  extends AbstractComputation<LongWritable, VertexValue, EdgeValue, PartitionMessage, PartitionMessage> {
    private Random rnd = new Random();
    private String[] loadAggregatorNames;
    private int numberOfPartitions;

    @Override
    public void compute(
        Vertex<LongWritable, VertexValue, EdgeValue> vertex,
        Iterable<PartitionMessage> messages) throws IOException {
      short partition = vertex.getValue().getCurrentPartition();
      if (partition == -1) {
        partition = (short) rnd.nextInt(numberOfPartitions);
      }
      aggregate(loadAggregatorNames[partition], new LongWritable(vertex.getNumEdges()));
      vertex.getValue().setCurrentPartition(partition);
      vertex.getValue().setNewPartition(partition);
      PartitionMessage message = new PartitionMessage(vertex.getId().get(), partition);
      sendMessageToAllEdges(vertex, message);
    }

    @Override
    public void preSuperstep() {
      numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
      loadAggregatorNames = new String[numberOfPartitions];
      for (int i = 0; i < numberOfPartitions; i++) {
        loadAggregatorNames[i] = AGGREGATOR_LOAD_PREFIX + i;
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
      numberOfPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
      convergenceThreshold = getContext().getConfiguration().getFloat(CONVERGENCE_THRESHOLD, DEFAULT_CONVERGENCE_THRESHOLD);
      // Create aggregators for each partition
      loadAggregatorNames = new String[numberOfPartitions];
      for (int i = 0; i < numberOfPartitions; i++) {
        loadAggregatorNames[i] = AGGREGATOR_LOAD_PREFIX + i;
        registerPersistentAggregator(loadAggregatorNames[i],
            LongSumAggregator.class);
        registerAggregator(AGGREGATOR_DEMAND_PREFIX + i,
            LongSumAggregator.class);
      }
      registerAggregator(AGGREGATOR_STATE,
          DoubleSumAggregator.class);
      registerAggregator(AGGREGATOR_LOCALS,
          LongSumAggregator.class);
      registerAggregator(AGGREGATOR_MIGRATIONS,
          LongSumAggregator.class);
    }

    private void printStats(int superstep) {
      System.out.println("superstep " + superstep);
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
        setComputation(Initializer.class);
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

  public static class PartitionMessage
  implements Writable {
    private long sourceId;
    private short partition;

    public PartitionMessage() { }

    public PartitionMessage(long sourceId, short partition) {
      this.sourceId = sourceId;
      this.partition = partition;
    }

    public long getSourceId() {
      return sourceId;
    }

    public void setSourceId(long sourceId) {
      this.sourceId = sourceId;
    }

    public short getPartition() {
      return partition;
    }

    public void setPartition(short partition) {
      this.partition = partition;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      sourceId = input.readLong();
      partition = input.readShort();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeLong(sourceId);
      output.writeShort(partition);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionMessage that = (PartitionMessage) o;
      if (partition != that.partition || sourceId != that.sourceId) {
        return false;
      }
      return true;
    }
  }

  public static class SoccPartitionStatsVertexOutputFormat extends
  TextVertexOutputFormat<LongWritable, VertexValue, EdgeValue> {
    /** Specify the output delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = " ";

    public TextVertexWriter
    createVertexWriter(TaskAttemptContext context) {
      return new TextIntIntVertexWriter();
    }

    protected class TextIntIntVertexWriter
    extends TextVertexWriterToEachLine {
      /** Saved delimiter */
      private String delimiter;

      @Override
      public void initialize(TaskAttemptContext context)
          throws IOException, InterruptedException {
        super.initialize(context);
        Configuration conf = context.getConfiguration();
        delimiter = conf
            .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      @Override
      protected Text convertVertexToLine
      (Vertex<LongWritable, VertexValue, EdgeValue> vertex)
          throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(vertex.getId().toString()).append(delimiter).
        append(vertex.getValue().getCurrentPartition());
        int numberOfEdges = vertex.getNumEdges();
        int localEdges = 0;
        for (Edge<LongWritable, EdgeValue> edge : vertex.getEdges()) {
          if (edge.getValue().getPartition() == vertex.getValue().getCurrentPartition()) {
            localEdges++;
          }
        }
        return new Text(sb.append(delimiter).
            append(localEdges).append(delimiter).
            append(numberOfEdges).toString());
      }
    }
  }

  public static class IdWithPartitionTextOutputFormat extends
  TextVertexOutputFormat<LongWritable, VertexValue, EdgeValue> {
    /** Specify the output delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public TextVertexWriter
    createVertexWriter(TaskAttemptContext context) {
      return new IdWithPartitionVertexWriter();
    }

    protected class IdWithPartitionVertexWriter
    extends TextVertexWriterToEachLine {
      /** Saved delimiter */
      private String delimiter;

      @Override
      public void initialize(TaskAttemptContext context)
          throws IOException, InterruptedException {
        super.initialize(context);
        Configuration conf = context.getConfiguration();
        delimiter = conf
            .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      @Override
      protected Text convertVertexToLine
      (Vertex<LongWritable, VertexValue, EdgeValue> vertex)
          throws IOException {
        return new Text(vertex.getId().get() + delimiter +
            vertex.getValue().getCurrentPartition());
      }
    }
  }

  public static class LongShortVertexValueInputFormat extends
  TextVertexValueInputFormat<LongWritable, VertexValue, EdgeValue> {
    private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

    @Override
    public LongShortTextVertexValueReader createVertexValueReader(
        InputSplit split, TaskAttemptContext context) throws IOException {
      return new LongShortTextVertexValueReader();
    }

    public class LongShortTextVertexValueReader extends
    TextVertexValueReaderFromEachLineProcessed<String[]> {

      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        return SEPARATOR.split(line.toString());
      }

      @Override
      protected LongWritable getId(String[] data) throws IOException {
        return new LongWritable(Long.parseLong(data[0]));
      }

      @Override
      protected VertexValue getValue(String[] data) throws IOException {
        VertexValue value = new VertexValue();
        if (data.length > 1) {
          short partition = Short.parseShort(data[1]);
          value.setCurrentPartition(partition);
          value.setNewPartition(partition);
        }
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
