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

import es.csic.iiia.bms.CommunicationAdapter;
import es.csic.iiia.bms.Factor;
import es.csic.iiia.bms.MaxOperator;
import es.csic.iiia.bms.Maximize;
import es.csic.iiia.bms.factors.ConditionedDeactivationFactor;
import es.csic.iiia.bms.factors.SelectorFactor;
import es.csic.iiia.bms.factors.WeightingFactor;
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
 * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
 * @author Toni Penya-Alba <tonipenya@iiia.csic.es>
 */
public class AffinityPropagation
    extends BasicComputation<APVertexID,
    APVertexValue, DoubleWritable, APMessage> {
  private static MaxOperator MAX_OPERATOR = new Maximize();

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
    logger.trace("vertex {}, superstep {}" , vertex.getId(), getSuperstep());
    final int maxIter = getContext().getConfiguration().getInt(MAX_ITERATIONS, MAX_ITERATIONS_DEFAULT);
    // Phases of the algorithm
    if (getSuperstep() == 0) {
      initRows(vertex);
    } else if (getSuperstep() == 1) {
      initColumns(vertex, messages);
    } else if (getSuperstep() < maxIter) {
      computeBMSIteration(vertex, messages);
    } else if (getSuperstep() == maxIter) {
      computeExemplars(vertex, messages);
    } else {
      computeClusters(vertex);
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
      APVertexID neighbor = new APVertexID(APVertexType.COLUMN, i);
      vertex.getValue().lastMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
    }
  }

  private void initRowsFromEdgeInput(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    for (Edge<APVertexID, DoubleWritable> edge : vertex.getEdges()) {
      APVertexID neighbor = new APVertexID(edge.getTargetVertexId());
      DoubleWritable weight = new DoubleWritable(edge.getValue().get());
      vertex.getValue().weights.put(neighbor, weight);
      vertex.getValue().lastMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
      removeEdgesRequest(vertex.getId(), neighbor);
    }
  }

  private void initColumns(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex, Iterable<APMessage> messages) {
    if (vertex.getId().type == APVertexType.ROW) {
      return;
    }

    for (APMessage message : messages) {
      APVertexID neighbor = new APVertexID(message.from);
      vertex.getValue().lastMessages.put(neighbor, new DoubleWritable(0));
      sendMessage(neighbor, new APMessage(vertex.getId(), 0));
    }
  }

  private void computeBMSIteration(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex,
                                   Iterable<APMessage> messages) throws IOException {
    final APVertexID id = vertex.getId();

    // Build a factor of the required type
    Factor<APVertexID> factor;
    switch (id.type) {

      case COLUMN:
        ConditionedDeactivationFactor<APVertexID> node2 = new ConditionedDeactivationFactor<APVertexID>();
        node2.setExemplar(new APVertexID(APVertexType.ROW, id.index));
        factor = node2;

        for (Writable key : vertex.getValue().lastMessages.keySet()) {
          APVertexID rowId = (APVertexID) key;
          logger.trace("{} adds neighbor {}", id, rowId);
          node2.addNeighbor(rowId);
        }
        break;

      case ROW:
        final MapWritable value = vertex.getValue().weights;

        SelectorFactor<APVertexID> selector = new SelectorFactor<APVertexID>();
        WeightingFactor<APVertexID> weights = new WeightingFactor<APVertexID>(selector);
        for (Writable key : vertex.getValue().lastMessages.keySet()) {
          APVertexID varId = (APVertexID) key;
          weights.addNeighbor(varId);
          weights.setPotential(varId, ((DoubleWritable) value.get(varId)).get());
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
      factor.receive(message.value, message.from);
    }
    factor.run();
  }

  private void computeExemplars(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex,
                                Iterable<APMessage> messages) throws IOException {
    final APVertexID id = vertex.getId();
    // Exemplars are auto-elected among variables
    if (id.type != APVertexType.COLUMN) {
      return;
    }

    // But only by those variables on the diagonal of the matrix
    for (APMessage message : messages) {
      if (message.from.index == id.index) {
        double lastMessageValue = ((DoubleWritable) vertex.getValue().lastMessages.get(message.from)).get();
        double belief = message.value + lastMessageValue;
        if (belief >= 0) {
          LongArrayListWritable exemplars = new LongArrayListWritable();
          exemplars.add(new LongWritable(id.index));
          aggregate("exemplars", exemplars);
          logger.trace("Point {} decides to become an exemplar with value {}.", id.index, belief);
        } else {
          logger.trace("Point {} does not want to be an exemplar with value {}.", id.index, belief);
        }

      }
    }

    vertex.voteToHalt();
  }

  private void computeClusters(Vertex<APVertexID, APVertexValue, DoubleWritable> vertex) throws IOException {
    APVertexID id = vertex.getId();
    if (id.type != APVertexType.ROW) {
      return;
    }

    final LongArrayListWritable exemplars = getAggregatedValue("exemplars");
    if (exemplars.contains(new LongWritable(id.index))) {
      logger.trace("Point {} is an exemplar.", id.index);
      vertex.getValue().exemplar = new LongWritable(id.index);
      vertex.voteToHalt();
      return;
    }

    long bestExemplar = -1;
    double maxValue = Double.NEGATIVE_INFINITY;
    MapWritable values = vertex.getValue().weights;
    for (LongWritable exemplarWritable : exemplars) {
      final long exemplar = exemplarWritable.get();
      final APVertexID neighbor = new APVertexID(APVertexType.COLUMN, exemplar);
      if (!values.containsKey(neighbor)) {
        continue;
      }

      final double value =  ((DoubleWritable) values.get(neighbor)).get();
      if (value > maxValue) {
        maxValue = value;
        bestExemplar = exemplar;
      }
    }

    logger.trace("Point {} decides to follow {}.", id.index, bestExemplar);
    vertex.getValue().exemplar = new LongWritable(bestExemplar);
    vertex.voteToHalt();
  }

  public class MessageRelayer implements CommunicationAdapter<APVertexID> {
    private MapWritable lastMessages;
    final float damping = getContext().getConfiguration().getFloat(DAMPING, DAMPING_DEFAULT);
    final float epsilon = getContext().getConfiguration().getFloat(EPSILON, EPSILON_DEFAULT);

    public MessageRelayer(MapWritable lastMessages) {
      this.lastMessages = lastMessages;
    }

    @Override
    public void send(double value, APVertexID sender, APVertexID recipient) {
      if (lastMessages.containsKey(recipient)) {
        final double lastMessage = ((DoubleWritable) lastMessages.get(recipient)).get();
        if (Math.abs(lastMessage - value) < epsilon) {
          return;
        }

        value = damping * lastMessage + (1-damping) * value;
      }
      logger.trace("{} -> {} : {}", sender, recipient, value);
      AffinityPropagation.this.sendMessage(recipient, new APMessage(sender, value));
      lastMessages.put(recipient, new DoubleWritable(value));
    }
  }

}
