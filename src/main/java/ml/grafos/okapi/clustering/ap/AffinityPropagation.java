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
package ml.grafos.okapi.clustering.ap;

import ml.grafos.okapi.common.data.LongArrayListWritable;
import ml.grafos.okapi.common.data.MapWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * Affinity Propagation is a clustering algorithm.
 *
 * <p>The number of clusters is not received as an input, but computed by the
 * algorithm according to the distances between points and the preference
 * of each node to be an <i>exemplar</i> (the "leader" of a cluster).
 *
 * <p>You can find a detailed description of the algorithm in the affinity propagation
 * <a href="http://genes.toronto.edu/index.php?q=affinity%20propagation">website</a>.
 *
 * @author Josep Rubió Piqué <josep@datag.es>
 */
public class AffinityPropagation
    extends BasicComputation<APVertexID,
    APVertexValue, DoubleWritable, APMessage> {

  private static Logger logger = LoggerFactory.getLogger(AffinityPropagation.class);

  /**
   * Maximum number of iterations.
   */
  public static final String MAX_ITERATIONS = "affinity.iterations";
  public static int MAX_ITERATIONS_DEFAULT = 15;
  /**
   * Damping factor.
   */
  public static final String DAMPING = "affinity.damping";
  public static float DAMPING_DEFAULT = 0.9f;
  /**
   * Noise factor.
   */
  public static final String NOISE = "affinity.noise";
  public static float NOISE_DEFAULT = 0f;
  /**
   * Epsilon factor. Do not send message to a neighbor if the new message has not changed more than epsilon.
   */
  public static final String EPSILON = "affinity.epsilon";
  public static float EPSILON_DEFAULT = 0.0001f;

  @Override
  public void compute(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex,
                      Iterable<APMessage> messages) throws IOException {
    logger.trace("vertex {}, superstep {}", vertex.getId(), getSuperstep());

    final int maxIter = getContext().getConfiguration().getInt(MAX_ITERATIONS, MAX_ITERATIONS_DEFAULT);

    // Phases of the algorithm

    for (APMessage message : messages) {
      vertex.getValue().lastReceivedMessages.put(new APVertexID(message.from), new DoubleWritable(message.value));
    }

    if(this.<LongWritable>getAggregatedValue("converged").get() == getTotalNumVertices()){
      if(!vertex.getValue().exemplarCalc.get()) {
        computeExemplars(vertex);
        vertex.getValue().exemplarCalc.set(true);
      }else{
        computeClusters(vertex);
      }
    }else{
      if (getSuperstep() == 0) {
        initRows(vertex);
      } else if (getSuperstep() == 1) {
        initColumns(vertex, messages);
      } else if (getSuperstep() < maxIter) {
        computeBMSIteration(vertex);
      } else if (getSuperstep() == maxIter) {
        computeExemplars(vertex);
      } else {
        computeClusters(vertex);
      }
    }
  }

  private void initRows(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    final boolean isVertexFormat = getConf().getVertexInputFormatClass() != null;
    if (isVertexFormat) {
      initRowsFromVertexInput(vertex);
    } else {
      initRowsFromEdgeInput(vertex);
    }
  }

  private void initRowsFromVertexInput(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) {
    final long nVertices = getTotalNumVertices();
    for (int i = 1; i <= nVertices; i++) {
      APVertexID neighbor = new APVertexID(APVertexType.E, i);
      vertex.getValue().lastSentMessages.put(neighbor, new DoubleWritable(0));
      vertex.getValue().lastReceivedMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
      logger.trace("Init rows: {} -> {} : {}", vertex.getId(), neighbor, 0);
    }
  }

  private void initRowsFromEdgeInput(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    for (Edge<APVertexID, DoubleWritable> edge : vertex.getEdges()) {
      APVertexID neighbor = new APVertexID(edge.getTargetVertexId());
      DoubleWritable weight = new DoubleWritable(edge.getValue().get());
      vertex.getValue().weights.put(neighbor, weight);
      vertex.getValue().lastSentMessages.put(neighbor, new DoubleWritable(0));
      vertex.getValue().lastReceivedMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
      logger.trace("Init rows:{} -> {} : {}", vertex.getId(), neighbor,  0);
      vertex.removeEdges(neighbor);
    }
  }

  private void initColumns(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex, Iterable<APMessage> messages) {
    if (vertex.getId().type == APVertexType.I) {
      final long nVertices = getTotalNumVertices();
      for (int i = 1; i <= nVertices; i++) {
        APVertexID neighbor = new APVertexID(APVertexType.E, i);
        vertex.getValue().lastSentMessages.put(neighbor, new DoubleWritable(0));
        vertex.getValue().lastReceivedMessages.put(neighbor, new DoubleWritable(0));
        sendMessage(neighbor, new APMessage(vertex.getId(), 0));
        logger.trace("Init columns:{} -> {} : {}", vertex.getId(), neighbor, 0);
      }
    }

    for (APMessage message : messages) {
      APVertexID neighbor = new APVertexID(message.from);
      vertex.getValue().lastSentMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
      logger.debug("Init columns:{} -> {} : {}", vertex.getId(), neighbor,  0);
    }
  }

  private void computeBMSIteration(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    final APVertexID id = vertex.getId();

    switch (id.type) {

      case E:
        computeEMessages(vertex);
        break;

      case I:
        computeIMessages(vertex);
        break;

      default:
        throw new IllegalStateException("Unrecognized node type " + id.type);
    }

  }

  private void computeEMessages(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) {

    double sum = 0;
    double exemplarMessage = 0;

    MessageRelayer sender = new MessageRelayer(vertex.getValue().lastSentMessages);

    for (Map.Entry<Writable, Writable> entry : vertex.getValue().lastReceivedMessages.entrySet()){
      APVertexID key = (APVertexID) entry.getKey();
      double messageValue = ((DoubleWritable) entry.getValue()).get();

      if (key.index != vertex.getId().index) {
        sum += Math.max(0, messageValue);
      } else {
        exemplarMessage = messageValue;
      }

    }

    for (Map.Entry<Writable, Writable> entry : vertex.getValue().lastReceivedMessages.entrySet()){
      APVertexID key = (APVertexID) entry.getKey();
      double messageValue = ((DoubleWritable) entry.getValue()).get();
      double value;

      if (key.index == vertex.getId().index) {
        value = sum;
      } else {
        double a = exemplarMessage + sum - Math.max(messageValue, 0);
        value = Math.min(0, a);
      }

      sender.send(value,vertex.getId(),key);

    }

    if (sender.isConverged()) {
      logger.debug("E vertex: {} converged.", vertex.getId());
      if(!vertex.getValue().converged.get()){
        vertex.getValue().converged.set(true);
        aggregate("converged", new LongWritable(1));
      }
    }else{
      if(vertex.getValue().converged.get()) {
        vertex.getValue().converged.set(false);
        aggregate("converged", new LongWritable(-1));
      }
    }

  }

  private void computeIMessages(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) {

    double bestValue = Double.NEGATIVE_INFINITY, secondBestValue = Double.NEGATIVE_INFINITY;
    APVertexID bestNeighbour = null;

    MessageRelayer sender = new MessageRelayer(vertex.getValue().lastSentMessages);

    MapWritable values = vertex.getValue().weights;

    for (Map.Entry<Writable, Writable> entry : vertex.getValue().lastReceivedMessages.entrySet()){
      APVertexID key = (APVertexID) entry.getKey();
      double messageValue = ((DoubleWritable) entry.getValue()).get();

      if (messageValue + ((DoubleWritable) values.get(key)).get() > bestValue) {
        secondBestValue = bestValue;
        bestValue = messageValue + ((DoubleWritable) values.get(key)).get();
        bestNeighbour = new APVertexID(key);
      }
      else if (messageValue + ((DoubleWritable) values.get(key)).get() > secondBestValue) {
        secondBestValue = messageValue + ((DoubleWritable) values.get(key)).get();
      }
    }

    for (Map.Entry<Writable, Writable> entry : vertex.getValue().lastReceivedMessages.entrySet()){
      APVertexID key = (APVertexID) entry.getKey();
      double value;

      if (key.equals(bestNeighbour)) {
        value = -secondBestValue;
      } else {
        value = -bestValue;
      }

      value = value + ((DoubleWritable) values.get(key)).get();
      sender.send(value,vertex.getId(),key);

    }

    if (sender.isConverged()) {
      logger.debug("I vertex: {} converged.", vertex.getId());
      if(!vertex.getValue().converged.get()){
        vertex.getValue().converged.set(true);
        aggregate("converged", new LongWritable(1));
      }
    }else{
      if(vertex.getValue().converged.get()) {
        vertex.getValue().converged.set(false);
        aggregate("converged", new LongWritable(-1));
      }
    }

  }

  private void computeExemplars(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    final APVertexID id = vertex.getId();
    // Exemplars are auto-elected among variables
    if (id.type != APVertexType.E) {
      return;
    }

    // But only by those variables on the diagonal of the matrix
    for (Map.Entry<Writable, Writable> entry : vertex.getValue().lastReceivedMessages.entrySet()){
      APVertexID key = (APVertexID) entry.getKey();
      double messageValue = ((DoubleWritable) entry.getValue()).get();

      if (key.index == id.index) {
        double lastMessageValue = ((DoubleWritable) vertex.getValue().lastSentMessages.get(key)).get();
        double belief = messageValue + lastMessageValue;
        if (belief >= 0) {
          LongArrayListWritable exemplars = new LongArrayListWritable();
          exemplars.add(new LongWritable(id.index));
          aggregate("exemplars", exemplars);
          logger.debug("Point {} decides to become an exemplar with value {}.", id.index, belief);
        } else {
          logger.debug("Point {} does not want to be an exemplar with value {}.", id.index, belief);
        }

      }
    }
  }

  private void computeClusters(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    APVertexID id = vertex.getId();
    if (id.type != APVertexType.I) {
      vertex.voteToHalt();
      return;
    }

    final LongArrayListWritable exemplars = getAggregatedValue("exemplars");
    if (exemplars.contains(new LongWritable(id.index))) {
      logger.debug("Point {} is an exemplar.", id.index);
      vertex.getValue().exemplar = new LongWritable(id.index);
      vertex.voteToHalt();
      return;
    }

    long bestExemplar = -1;
    double maxValue = Double.NEGATIVE_INFINITY;
    MapWritable values = vertex.getValue().weights;
    for (LongWritable exemplarWritable : exemplars) {
      final long exemplar = exemplarWritable.get();
      final APVertexID neighbor = new APVertexID(APVertexType.E, exemplar);
      if (!values.containsKey(neighbor)) {
        continue;
      }

      final double value =  ((DoubleWritable) values.get(neighbor)).get();
      if (value > maxValue) {
        maxValue = value;
        bestExemplar = exemplar;
      }
    }

    logger.debug("Point {} decides to follow {}.", id.index, bestExemplar);
    vertex.getValue().exemplar = new LongWritable(bestExemplar);
    vertex.voteToHalt();
  }

  public class MessageRelayer{
    private MapWritable lastMessages;
    final float damping = getContext().getConfiguration().getFloat(DAMPING, DAMPING_DEFAULT);
    final float epsilon = getContext().getConfiguration().getFloat(EPSILON, EPSILON_DEFAULT);
    int nMessagesSent = 0;

    public MessageRelayer(MapWritable lastMessages) {
      this.lastMessages = lastMessages;
    }

    public void send(double value, APVertexID sender, APVertexID recipient) {
      if (lastMessages.containsKey(recipient)) {
        final double lastMessage = ((DoubleWritable) lastMessages.get(recipient)).get();
        if (Math.abs(lastMessage - value) < epsilon) {
          return;
        }

        value = damping * lastMessage + (1 - damping) * value;
      }
      logger.trace("{} -> {} : {}", sender, recipient, value);
      AffinityPropagation.this.sendMessage(recipient, new APMessage(sender, value));
      lastMessages.put(recipient, new DoubleWritable(value));
      nMessagesSent++;
    }

    public boolean isConverged() {
      return nMessagesSent == 0;
    }
  }

}
