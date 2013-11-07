package ml.grafos.okapi.cf.sgd;

import java.util.Map.Entry;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import ml.grafos.okapi.utils.TextMessageWrapper;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


/**
 * Demonstrates the Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Stochastic Gradient Descent (SGD)",
  description = "Minimizes the error in users preferences predictions")

public class Sgd extends BasicComputation<Text, SgdVertexValue,
DoubleWritable, TextMessageWrapper> {
  /** Keyword for RMSE aggregator tolerance. */
  public static final String RMSE_TARGET = "sgd.rmse.target";
  /** Default value for parameter enabling the RMSE aggregator. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
  /** Keyword for parameter setting the convergence tolerance parameter
   *  depending on the version enabled; l2norm or rmse. */
  public static final String TOLERANCE = "sgd.halting.tolerance";
  /** Default value for TOLERANCE. */
  public static final float TOLERANCE_DEFAULT = 1f;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS = "sgd.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the regularization parameter LAMBDA. */
  public static final String LAMBDA = "sgd.lambda";
  /** Default value for LABDA. */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the learning rate GAMMA. */
  public static final String GAMMA = "sgd.gamma";
  /** Default value for GAMMA. */
  public static final float GAMMA_DEFAULT = 0.005f;
  /** Keyword for parameter setting the Latent Vector Size. */
  public static final String VECTOR_SIZE = "sgd.vector.size";
  /** Default value for GAMMA. */
  public static final int VECTOR_SIZE_DEFAULT = 2;
  
  /** Max rating. */
  public static final double MAX_RATING = 5;
  /** Min rating. */
  public static final double MIN_RATING = 0;
  /** Number used in the initialization of values. */
  public static final double HUNDRED = 100;
  
  /** Aggregator used to compute the RMSE */
  public static final String RMSE_AGGREGATOR = "sgd.rmse.aggregator";
  
  /** Factor Error: it may be RMSD or L2NORM on initial & final vector. */
  private double haltFactor = 0d;
//  /** Number of updates - used in the Output Format. */
//  private int updatesNum = 0;
  /** Type of vertex 0 for user, 1 for item - used in the Output Format. */
  private boolean isItem = false;
  /**
   * Initial vector value to be used for the L2Norm case.
   * Keep it outside the compute() method
   * value has to be preserved throughout the supersteps.
   */
  private DoubleArrayListWritable initialValue;
  /** Counter of messages received.
   * This is different from getNumEdges() because a
   * neighbor may not send a message
   */
  private int messagesNum = 0;

  /**
   * Compute method.
   * @param messages Messages received
   */
  public final void compute(Vertex<Text, SgdVertexValue,
      DoubleWritable> vertex, final Iterable<TextMessageWrapper> messages) {

    /* Flag for checking if parameter for RMSE aggregator received */
//    float rmseTarget = getContext().getConfiguration().getFloat(
//      RMSE_TARGET, RMSE_TARGET_DEFAULT);
    
    int iterations = getContext().getConfiguration().getInt(
      ITERATIONS, ITERATIONS_DEFAULT);
    float tolerance = getContext().getConfiguration().getFloat(
      TOLERANCE, TOLERANCE_DEFAULT);
    float lambda = getContext().getConfiguration().getFloat(
      LAMBDA, LAMBDA_DEFAULT);
    float gamma = getContext().getConfiguration().getFloat(
      GAMMA, GAMMA_DEFAULT);
    int vectorSize = getContext().getConfiguration().getInt(
      VECTOR_SIZE, VECTOR_SIZE_DEFAULT);
    
    // First superstep for users (superstep 0) & items (superstep 1)
    // Initialize vertex latent vector
    if (getSuperstep() < 2) {
      initLatentVector(vertex, vectorSize);
      // For L2Norm
      initialValue = new DoubleArrayListWritable(
          vertex.getValue().getLatentVector());
    }
    // Set flag for items - used in the Output Format
    if (getSuperstep() == 1) {
      isItem = true;
    }

    // Used if RMSE version or RMSE aggregator is enabled
    double rmsePartialSum = 0d;

    for (TextMessageWrapper message : messages) {
      messagesNum++;
      // First superstep for items:
      // 1. Create outgoing edges of items
      // 2. Store the rating given from users in the outgoing edges
      if (getSuperstep() == 1) {
        double observed = message.getMessage().get(
          message.getMessage().size() - 1).get();
        DefaultEdge<Text, DoubleWritable> edge =
          new DefaultEdge<Text, DoubleWritable>();
        edge.setTargetVertexId(message.getSourceId());
        edge.setValue(new DoubleWritable(observed));
        vertex.addEdge(edge);
        // Remove the last value from message
        // It's there only for the 1st round of items
        message.getMessage().remove(message.getMessage().size() - 1);
      }

      // If delta caching is NOT enabled
      // Calculate error
      double observed = (double) vertex.getEdgeValue(
          message.getSourceId()).get();
      double err = getError(vertex.getValue().getLatentVector(),
          message.getMessage(), observed);
      // Change the Vertex Latent Vector based on SGD equation
      updateValue(vertex, message.getMessage(), lambda, gamma, err);
      err = getError(vertex.getValue().getLatentVector(),
          message.getMessage(),
          observed);
      
      // If termination flag is set to RMSE OR RMSE aggregator is enabled
      if (factorFlag.equals("rmse") || rmseTarget != 0f) {
        rmsePartialSum += Math.pow(err, 2);
      }
    } 

    haltFactor = getL2Norm(initialValue, vertex.getValue().getLatentVector());

    // If RMSE aggregator flag is true - send rmseErr to aggregator
    if (rmseTarget != 0f) {
      this.aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmsePartialSum));
    }

    if (getSuperstep() == 0
      ||
      (haltFactor > tolerance && getSuperstep() < iterations)) {
      sendMessage(vertex);
    }
    
    // haltFactor is used in the OutputFormat file. --> To print the error
//    if (factorFlag.equals("basic")) {
//      haltFactor = err;
//    }
    
    vertex.voteToHalt();
  }

  /**
   * Return type of current vertex.
   *
   * @return item
   */
  public final boolean isItem() {
    return isItem;
  }

  /**
   * Initialize Vertex Latent Vector.
   *
   * @param vectorSize Size of latent vector
   */
  public final void initLatentVector(Vertex<Text, SgdVertexValue,
      DoubleWritable> vertex, final int vectorSize) {
    SgdVertexValue value =
      new SgdVertexValue();
    for (int i = 0; i < vectorSize; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((Double.parseDouble(vertex.getId().toString().substring(2)) + i) % HUNDRED)
        / HUNDRED));
    }
    vertex.setValue(value);
  }

  /**
   * Modify Vertex Latent Vector based on SGD equation.
   *
   * @param vvertex Vertex value
   * @param lambda Regularization parameter
   * @param gamma Larning rate
   * @param err Error between predicted and observed rating
   */
  public final void updateValue(Vertex<Text, SgdVertexValue,
      DoubleWritable> vertex,
    final DoubleArrayListWritable vvertex, final float lambda,
    final float gamma, final double err) {
    /**
     * vertex_vector = vertex_vector + part3
     *
     * part1 = LAMBDA * vertex_vector
     * part2 = real_value - dot_product(vertex_vector,other_vertex_vector)) *
     * other_vertex_vector
     * part3 = - GAMMA * (part1 + part2)
     */
    DoubleArrayListWritable part1;
    DoubleArrayListWritable part2;
    DoubleArrayListWritable part3;
    DoubleArrayListWritable value;
    part1 = numMatrixProduct((double) lambda,
      vertex.getValue().getLatentVector());
    part2 = numMatrixProduct((double) err, vvertex);
    part3 = numMatrixProduct((double) -gamma,
      dotAddition(part1, part2));
    value = dotAddition(vertex.getValue().getLatentVector(), part3);
    vertex.getValue().setLatentVector(value);
//    updatesNum++;
  }

  /**
   * Send messages to neighbours.
   */
  public final void sendMessage(Vertex<Text, SgdVertexValue,
      DoubleWritable> vertex) {
    // Create a message and wrap together the source id and the message
    TextMessageWrapper message = new TextMessageWrapper();
    message.setSourceId(vertex.getId());
    // At superstep 0, users send rating to items
    if (getSuperstep() == 0) {
      for (Edge<Text, DoubleWritable> edge : vertex.getEdges()) {
        DoubleArrayListWritable x = 
            new DoubleArrayListWritable(vertex.getValue().getLatentVector());
        x.add(new DoubleWritable(edge.getValue().get()));
        message.setMessage(x);
        sendMessage(edge.getTargetVertexId(), message);
      }
    } else {
      message.setMessage(vertex.getValue().getLatentVector());
      sendMessageToAllEdges(vertex, message);
    }
  }

  /**
   * Calculate the RMSE on the errors calculated by the current vertex.
   *
   * @param rmseErr RMSE error
   * @return RMSE result
   */
  public final double getRMSE(final double rmseErr) {
    return Math.sqrt(rmseErr / (double) messagesNum);
  }

  /** Calculate the L2Norm on the initial and final value of vertex.
   *
   * @param valOld Old value
   * @param valNew New value
   * @return result of L2Norm equation
   * */
  public final double getL2Norm(final DoubleArrayListWritable valOld,
    final DoubleArrayListWritable valNew) {
    double result = 0;
    for (int i = 0; i < valOld.size(); i++) {
      result += Math.pow(valOld.get(i).get() - valNew.get(i).get(), 2);
    }
    return Math.sqrt(result);
  }

  /**
   * Calculate the error: e = observed - predicted.
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @param observed Observed value
   * @return Result from deducting observed value from predicted
   */
  public final double getError(final DoubleArrayListWritable vectorA,
    final DoubleArrayListWritable vectorB, final double observed) {
    double predicted = dotProduct(vectorA, vectorB);
    predicted = Math.min(predicted, MAX_RATING);
    predicted = Math.max(predicted, MIN_RATING);
    return predicted - observed;
  }

  /**
   * Calculate the dot product of 2 vectors: vector1 * vector2.
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return Result from dot product of 2 vectors
   */
  public final double dotProduct(final DoubleArrayListWritable vectorA,
    final DoubleArrayListWritable vectorB) {
    double result = 0d;
    for (int i = 0; i < vectorA.size(); i++) {
      result += vectorA.get(i).get() * vectorB.get(i).get();
    }
    return result;
  }

  /**
   * Calculate the dot addition of 2 vectors: vectorA + vectorB.
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return result Result from dot addition of the two vectors
   */
  public final DoubleArrayListWritable dotAddition(
    final DoubleArrayListWritable vectorA,
    final DoubleArrayListWritable vectorB) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < vectorA.size(); i++) {
      result.add(new DoubleWritable(
        vectorA.get(i).get() + vectorB.get(i).get()));
    }
    return result;
  }

  /**
   * Calculate the product num * matirx.
   *
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   */
  public final DoubleArrayListWritable numMatrixProduct(
    final double num, final DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < matrix.size(); i++) {
      result.add(new DoubleWritable(num * matrix.get(i).get()));
    }
    return result;
  }

//  /**
//   * Return amount of vertex updates.
//   *
//   * @return updatesNum
//   * */
//  public final int getUpdates() {
//    return updatesNum;
//  }

  /**
   * Return amount messages received.
   *
   * @return messagesNum
   * */
  public final int getMessages() {
    return messagesNum;
  }

  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute
  extends DefaultMasterCompute {
    @Override
    public final void compute() {
      // Set the Convergence Tolerance
      float rmseTolerance = getContext().getConfiguration()
        .getFloat(RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
      double numRatings = 0;
      double totalRMSE = 0;

      if (getSuperstep() > 1) {
        // In superstep=1 only half edges are created (users to items)
        if (getSuperstep() == 2) {
          numRatings = getTotalNumEdges();
        } else {
          numRatings = getTotalNumEdges() / 2;
        }
      }
      if (rmseTolerance != 0f) {
        totalRMSE = Math.sqrt(((DoubleWritable)
          getAggregatedValue(RMSE_AGGREGATOR)).get() / numRatings);

        System.out.println("SS:" + getSuperstep() + ", Total RMSE: "
          +
          totalRMSE + " = sqrt(" + getAggregatedValue(RMSE_AGGREGATOR)
          +
          " / " + numRatings + ")");
      }
      if (totalRMSE < rmseTolerance) {
        haltComputation();
      }
    }

    @Override
    public final void initialize() throws InstantiationException,
    IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
    }
  }
}
