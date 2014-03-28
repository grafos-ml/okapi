/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs;

import es.csic.iiia.bms.CommunicationAdapter;
import es.csic.iiia.bms.Factor;
import es.csic.iiia.bms.MaxOperator;
import es.csic.iiia.bms.Maximize;
import es.csic.iiia.bms.factors.ConditionedDeactivationFactor;
import es.csic.iiia.bms.factors.SelectorFactor;
import es.csic.iiia.bms.factors.WeightingFactor;
import ml.grafos.okapi.common.data.*;
import ml.grafos.okapi.common.data.MapWritable;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Affinity Propagation is a clustering algorithm.
 * <p/>
 * The number of clusters is not received as an input, but computed by the
 * algorithm according to the distances between points and the preference
 * of each node to be an <i>exemplar</i> (the "leader" of a cluster).
 * <p/>
 * You can find a detailed description of the algorithm in the affinity propagation
 * <a href="http://genes.toronto.edu/index.php?q=affinity%20propagation">website</a>.
 *
 * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
 * @author Toni Penya-Alba <tonipenya@iiia.csic.es>
 */
public class AffinityPropagation
    extends BasicComputation<AffinityPropagation.APVertexID,
    AffinityPropagation.APVertexValue, NullWritable, AffinityPropagation.APMessage> {
  private static MaxOperator MAX_OPERATOR = new Maximize();

  private static Logger logger = Logger.getLogger(AffinityPropagation.class);

  /**
   * Maximum number of iterations.
   */
  public static final String MAX_ITERATIONS = "iterations";
  public static int MAX_ITERATIONS_DEFAULT = 15;
  /**
   * Damping factor.
   */
  public static final String DAMPING = "damping";
  public static float DAMPING_DEFAULT = 0.9f;

  @Override
  public void compute(Vertex<APVertexID, APVertexValue, NullWritable> vertex,
                      Iterable<APMessage> messages) throws IOException {
    logger.trace("vertex " + vertex.getId() + ", superstep " + getSuperstep());
    final int maxIter = getContext().getConfiguration().getInt(MAX_ITERATIONS, MAX_ITERATIONS_DEFAULT);
    // Phases of the algorithm
    if (getSuperstep() == 0) {
      computeRowsColumns(vertex, messages);
    } else if (getSuperstep() < maxIter) {
      computeBMSIteration(vertex, messages);
    } else if (getSuperstep() == maxIter) {
      computeExemplars(vertex, messages);
    } else {
      computeClusters(vertex, messages);
    }
  }

  private void computeRowsColumns(Vertex<APVertexID, APVertexValue, NullWritable> vertex,
                                  Iterable<APMessage> messages) throws IOException {
    final APVertexID id = vertex.getId();
    aggregate("nRows", new LongWritable(id.row));
    aggregate("nColumns", new LongWritable(id.column));
  }

  private void computeBMSIteration(Vertex<APVertexID, APVertexValue, NullWritable> vertex,
                                   Iterable<APMessage> messages) throws IOException {
    final APVertexID id = vertex.getId();

    LongWritable aggregatedRows = getAggregatedValue("nRows");
    final long nRows = aggregatedRows.get();
    LongWritable aggregatedColumns = getAggregatedValue("nRows");
    final long nColumns = aggregatedColumns.get();
    if (nRows != nColumns) {
      throw new IllegalStateException("The input must form a square matrix, but we got " +
          nRows + " rows and " + nColumns + "columns.");
    }

    if (getSuperstep() == 1) {
      logger.trace("Number of rows: " + nRows);
      logger.trace("Number of columns: " + nColumns);
    }

    // Build a factor of the required type
    Factor<APVertexID> factor;
    switch (id.type) {

      case CONSISTENCY:
        ConditionedDeactivationFactor<APVertexID> node2 = new ConditionedDeactivationFactor<APVertexID>();
        node2.setExemplar(new APVertexID(APVertexType.SELECTOR, id.column, 0));
        factor = node2;

        for (int row = 1; row <= nRows; row++) {
          APVertexID varId = new APVertexID(APVertexType.SELECTOR, row, 0);
          node2.addNeighbor(varId);
        }

        break;

      case SELECTOR:
        final DoubleArrayListWritable value = vertex.getValue().weights;
        SelectorFactor<APVertexID> selector = new SelectorFactor<APVertexID>();
        WeightingFactor<APVertexID> weights = new WeightingFactor<APVertexID>(selector);
        for (int column = 1; column <= nColumns; column++) {
          APVertexID varId = new APVertexID(APVertexType.CONSISTENCY, 0, column);
          weights.addNeighbor(varId);
          weights.setPotential(varId, value.get(column - 1).get());
        }
        factor = weights;
        break;

      default:
        throw new IllegalStateException("Unrecognized node type " + id.type);
    }

    // Initialize it with proper values
    MessageRelayer collector = new MessageRelayer(vertex.getValue().lastMessages);
    factor.setCommunicationAdapter(collector);
    factor.setIdentity(id);
    factor.setMaxOperator(MAX_OPERATOR);

    // Receive messages and compute
    for (APMessage message : messages) {
      logger.trace(message);
      factor.receive(message.value, message.from);
    }
    factor.run();
  }

  private void computeExemplars(Vertex<APVertexID, APVertexValue, NullWritable> vertex,
                                Iterable<APMessage> messages) throws IOException {
    final APVertexID id = vertex.getId();
    // Exemplars are auto-elected among variables
    if (id.type != APVertexType.CONSISTENCY) {
      return;
    }

    // But only by those variables on the diagonal of the matrix
    for (APMessage message : messages) {
      if (message.from.row == id.column) {
        double lastMessageValue = ((DoubleWritable) vertex.getValue().lastMessages.get(message.from)).get();
        double belief = message.value + lastMessageValue;
        if (belief >= 0) {
          LongArrayListWritable exemplars = new LongArrayListWritable();
          exemplars.add(new LongWritable(id.column));
          aggregate("exemplars", exemplars);
          logger.trace("Point " + id.column + " decides to become an exemplar with value " + belief + ".");
        } else {
          logger.trace("Point " + id.column + " does not want to be an exemplar with value " + belief + ".");
        }

      }
    }

    vertex.voteToHalt();
  }

  private void computeClusters(Vertex<APVertexID, APVertexValue, NullWritable> vertex,
                               Iterable<APMessage> messages) throws IOException {
    APVertexID id = vertex.getId();
    if (id.type != APVertexType.SELECTOR) {
      return;
    }

    final LongArrayListWritable ls = getAggregatedValue("exemplars");
    DoubleArrayListWritable values = vertex.getValue().weights;
    double maxValue = Double.NEGATIVE_INFINITY;
    long bestExemplar = -1;
    for (LongWritable e : ls) {
      final long exemplar = e.get();

      if (exemplar == id.row) {
        logger.trace("Point " + id.row + " is an exemplar.");
        vertex.getValue().exemplar = new LongWritable(id.row);
        vertex.voteToHalt();
        return;
      }

      final double value = values.get((int) (exemplar - 1)).get();
      if (value > maxValue) {
        maxValue = value;
        bestExemplar = exemplar;
      }
    }

    logger.trace("Point " + id.row + " decides to follow " + bestExemplar + ".");
    vertex.getValue().exemplar = new LongWritable(bestExemplar);
    vertex.voteToHalt();
  }

  public static enum APVertexType {
    CONSISTENCY, SELECTOR
  }

  public static class APVertexID implements WritableComparable<APVertexID> {

    public APVertexType type = APVertexType.SELECTOR;
    public long row = 0;
    public long column = 0;

    public APVertexID() {
    }

    public APVertexID(APVertexType type, long row, long column) {
      this.type = type;
      this.row = row;
      this.column = column;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(type.ordinal());
      dataOutput.writeLong(row);
      dataOutput.writeLong(column);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      final int index = dataInput.readInt();
      type = APVertexType.values()[index];
      row = dataInput.readLong();
      column = dataInput.readLong();
    }

    @Override
    public int compareTo(APVertexID o) {
      if (o == null) {
        return 1;
      }

      if (!type.equals(o.type)) {
        return type.compareTo(o.type);
      }

      if (row != o.row) {
        return Long.compare(row, o.row);
      }

      if (column != o.column) {
        return Long.compare(column, o.column);
      }

      return 0;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      APVertexID that = (APVertexID) o;

      if (column != that.column) return false;
      if (row != that.row) return false;
      if (type != that.type) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + (int) (row ^ (row >>> 32));
      result = 31 * result + (int) (column ^ (column >>> 32));
      return result;
    }

    @Override
    public String toString() {
      return "(" + type + ", " + row + ", " + column + ")";
    }
  }

  public static class APVertexValue implements Writable {
    public LongWritable exemplar;
    public DoubleArrayListWritable weights;
    public MapWritable lastMessages;

    public APVertexValue() {
      exemplar = new LongWritable();
      weights = new DoubleArrayListWritable();
      lastMessages = new MapWritable();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      exemplar.write(dataOutput);
      weights.write(dataOutput);
      lastMessages.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      exemplar.readFields(dataInput);
      weights.readFields(dataInput);
      lastMessages.readFields(dataInput);
    }
  }

  public static class APMessage implements Writable {

    public APVertexID from;
    public double value;

    public APMessage() {
      from = new APVertexID();
    }

    public APMessage(APVertexID from, double value) {
      this.from = from;
      this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      from.write(dataOutput);
      dataOutput.writeDouble(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      from.readFields(dataInput);
      value = dataInput.readDouble();
    }

    @Override
    public String toString() {
      return "APMessage{from=" + from + ", value=" + value + '}';
    }
  }

  public class MessageRelayer implements CommunicationAdapter<APVertexID> {
    private MapWritable lastMessages;
    final float damping = getContext().getConfiguration().getFloat(DAMPING, DAMPING_DEFAULT);

    public MessageRelayer(MapWritable lastMessages) {
      this.lastMessages = lastMessages;
    }

    @Override
    public void send(double value, APVertexID sender, APVertexID recipient) {
      if (lastMessages.containsKey(recipient)) {
        final double lastMessage = ((DoubleWritable) lastMessages.get(recipient)).get();
        value = damping * lastMessage + (1-damping) * value;
      }
      logger.trace(sender + " -> " + recipient + " : " + value);
      AffinityPropagation.this.sendMessage(recipient, new APMessage(sender, value));
      lastMessages.put(recipient, new DoubleWritable(value));
    }
  }

  public static class MasterComputation extends DefaultMasterCompute {

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
      super.initialize();

      registerPersistentAggregator("nRows", LongMaxAggregator.class);
      registerPersistentAggregator("nColumns", LongMaxAggregator.class);
      registerPersistentAggregator("exemplars", ExemplarAggregator.class);
    }

  }

  public static class ExemplarAggregator extends BasicAggregator<LongArrayListWritable> {
    @Override
    public void aggregate(LongArrayListWritable value) {
      getAggregatedValue().addAll(value);
    }

    @Override
    public LongArrayListWritable createInitialValue() {
      return new LongArrayListWritable();
    }
  }

  /**
   * Input formatter for Affinity Propagation problems.
   * <p/>
   * The input format consists of an entry for each of the data points to cluster.
   * The first element of the entry is an integer value encoding the data point
   * index (id). Subsequent elements in the entry are double values encoding the
   * similarities between the data point of the current entry and the rest of
   * data points in the problem.
   * <p/>
   * Example:<br/>
   * 1 1 1 5
   * 2 1 1 3
   * 3 5 3 1
   * <p/>
   * Encodes a problem in which data point "1" has similarity 1 with itself,
   * 1 with point "2" and 5 with point "3". In a similar manner, points "2",
   * and "3" have similarities of [1, 1, 3] and [5, 3, 1] respectively with
   * points "1", "2", and "3".
   *
   * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
   * @author Toni Penya-Alba <tonipenya@iiia.csic.es>
   */
  public static class APInputFormatter
      extends TextVertexValueInputFormat<APVertexID, APVertexValue, NullWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

    @Override
    public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
      return new APInputReader();
    }

    public class APInputReader extends TextVertexValueReaderFromEachLineProcessed<String[]> {

      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        return SEPARATOR.split(line.toString());
      }

      @Override
      protected APVertexID getId(String[] line) throws IOException {
        return new APVertexID(APVertexType.SELECTOR,
            Long.valueOf(line[0]), 0);
      }

      @Override
      protected APVertexValue getValue(String[] line) throws IOException {
        APVertexValue value = new APVertexValue();
        for (int i = 1; i < line.length; i++) {
          value.weights.add(new DoubleWritable(Double.valueOf(line[i])));
        }
        return value;
      }
    }
  }

  /**
   * Output Formatter for Affinity Propagation problems.
   * <p/>
   * The output format consists of an entry for each of the data points to cluster.
   * The first element of the entry is a integer value encoding the data point
   * index (id), whereas the second value encodes the exemplar id chosen for
   * that point.
   * <p/>
   * Example:<br/>
   * 1 3
   * 2 3
   * 3 3
   * <p/>
   * Encodes a solution in which data points "1", "2", and "3" choose point "3"
   * as an exemplar.
   *
   * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
   * @author Toni Penya-Alba <tonipenya@iiia.csic.es>
   */
  @SuppressWarnings("rawtypes")
  public static class APOutputFormat
      extends IdWithValueTextOutputFormat<APVertexID,APVertexValue, NullWritable> {

    /**
     * Specify the output delimiter
     */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /**
     * Default output delimiter
     */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
      return new IdWithValueVertexWriter();
    }

    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
      /**
       * Saved delimiter
       */
      private String delimiter;

      @Override
      public void initialize(TaskAttemptContext context) throws IOException,
          InterruptedException {
        super.initialize(context);
        delimiter = getConf().get(
            LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      @Override
      protected Text convertVertexToLine(Vertex<APVertexID,
          APVertexValue, NullWritable> vertex)
          throws IOException {

        if (vertex.getId().type != APVertexType.SELECTOR) {
          return null;
        }

        StringBuilder str = new StringBuilder();
        str.append(vertex.getId().row);
        str.append(delimiter);
        str.append(Long.toString(vertex.getValue().exemplar.get()));
        return new Text(str.toString());
      }
    }
  }
}
