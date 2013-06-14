package es.tid.graphlib.sgd;

import java.util.Map.Entry;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import es.tid.graphlib.utils.DoubleArrayListHashMapWritable;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Stochastic Gradient Descent (SGD)",
  description = "Minimizes the error in users preferences predictions")

public class Sgd extends Vertex<IntWritable, DoubleArrayListHashMapWritable,
DoubleWritable, MessageWrapper> {
  /** Keyword for parameter enabling delta caching */
  public static final String DELTA_CACHING = "sgd.delta.caching";
  /** Default value for parameter enabling delta caching */
  public static final boolean DELTA_CACHING_DEFAULT = false;
  /** Keyword for RMSE aggregator tolerance */
  public static final String RMSE_AGGREGATOR = "sgd.rmse.aggregator";
  /** Default value for parameter enabling the RMSE aggregator */
  public static final float RMSE_AGGREGATOR_DEFAULT = 0f;
  /** Keyword for parameter choosing the halt factor */
  public static final String HALT_FACTOR = "sgd.halt.factor";
  /** Default value for parameter choosing the halt factor */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Keyword for parameter setting the convergence tolerance parameter
   *  depending on the version enabled; l2norm or rmse */
  public static final String TOLERANCE_KEYWORD = "sgd.halting.tolerance";
  /** Default value for TOLERANCE */
  public static final float TOLERANCE_DEFAULT = 1;
  /** Keyword for parameter setting the number of iterations */
  public static final String ITERATIONS_KEYWORD = "sgd.iterations";
  /** Default value for ITERATIONS */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Regularization parameter LAMBDA */
  public static final String LAMBDA_KEYWORD = "sgd.lambda";
  /** Default value for LABDA */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the learning rate GAMMA */
  public static final String GAMMA_KEYWORD = "sgd.gamma";
  /** Default value for GAMMA */
  public static final float GAMMA_DEFAULT = 0.005f;
  /** Keyword for parameter setting the Latent Vector Size */
  public static final String VECTOR_SIZE_KEYWORD = "sgd.vector.size";
  /** Default value for GAMMA */
  public static final int VECTOR_SIZE_DEFAULT = 2;
  /** Max rating */
  public static final double MAX = 5;
  /** Min rating */
  public static final double MIN = 0;
  /** Decimals */
  public static final int DECIMALS = 4;
  /** Factor Error: it may be RMSD or L2NORM on initial&final vector */
  private double haltFactor = 0d;
  /** Number of updates - used in the Output Format */
  private int updatesNum = 0;
  /** Type of vertex 0 for user, 1 for item - used in the Output Format*/
  private boolean isItem = false;
  /**
   * Initial vector value to be used for the L2Norm case
   * Keep it outside the compute() method
   * value has to preserved throughout the supersteps
   */
  DoubleArrayListWritable initialValue = new DoubleArrayListWritable();
  /**
   * Counter of messages received
   * This is different from getNumEdges() because a
   * neighbor may not send a message
   */
  private int messagesNum = 0;
  
  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<MessageWrapper> messages) {   
    /** Error between predicted and observed rating */
    double err = 0d;

    /* Flag for checking if parameter for RMSE aggregator received */
    float rmseTolerance = getContext().getConfiguration().getFloat(
      RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
    /*
     * Flag for checking which termination factor to use:
     * basic, rmse, l2norm
     */
    String factorFlag = getContext().getConfiguration().get(HALT_FACTOR,
      HALT_FACTOR_DEFAULT);
    /* Flag for checking if delta caching is enabled */
    boolean isDeltaEnabled = getContext().getConfiguration().getBoolean(
      DELTA_CACHING, DELTA_CACHING_DEFAULT);
    /* Set the number of iterations */
    int iterations = getContext().getConfiguration().getInt(ITERATIONS_KEYWORD,
      ITERATIONS_DEFAULT);
    /* Set the Convergence Tolerance */
    float tolerance = getContext().getConfiguration()
      .getFloat(TOLERANCE_KEYWORD, TOLERANCE_DEFAULT);
    /* Set the Regularization Parameter LAMBDA */
    float lambda = getContext().getConfiguration()
      .getFloat(LAMBDA_KEYWORD, LAMBDA_DEFAULT);
    /* Set the Learning Rate GAMMA */
    float gamma = getContext().getConfiguration()
      .getFloat(GAMMA_KEYWORD, GAMMA_DEFAULT);
    /* Set the size of the Latent Vector*/
    int vectorSize = getContext().getConfiguration()
      .getInt(VECTOR_SIZE_KEYWORD, VECTOR_SIZE_DEFAULT);
    /* Flag becomes true if at least one neighbour latent vector gets updated */
    boolean isNeighUpdated = false;

    // First superstep for users (superstep 0) & items (superstep 1)
    // Initialize vertex latent vector
    if (getSuperstep() < 2) {
      initLatentVector(vectorSize);
      // For L2Norm
      initialValue = getValue().getLatentVector();
    }
    // Set flag for items - used in the Output Format
    if (getSuperstep() == 1) {
      isItem = true;
    }

    // Used if RMSE version or RMSE aggregator is enabled
    double rmseErr = 0d;

    // FOR LOOP - for each message
    for (MessageWrapper message : messages) {
      messagesNum++;
      // First superstep for items:
      // 1. Create outgoing edges of items
      // 2. Store the rating given from users in the outgoing edges
      if (getSuperstep() == 1) {
        double observed = message.getMessage().get(message.getMessage().size() - 1)
          .get();
        DefaultEdge<IntWritable, DoubleWritable> edge =
          new DefaultEdge<IntWritable, DoubleWritable>();
        edge.setTargetVertexId(message.getSourceId());
        edge.setValue(new DoubleWritable(observed));
        addEdge(edge);
        // Remove the last value from message
        // It's there only for the 1st round of items
        message.getMessage().remove(message.getMessage().size() - 1);
      } /* END OF IF CLAUSE - superstep==1 */

      // IF (delta caching is enabled) THEN
      // For the 1st superstep of either user or item: initialize their values
      // For any superstep > 0:
      // update their values based on the message received
      if (isDeltaEnabled) {
        DoubleArrayListWritable currVal =
          getValue().getNeighValue(message.getSourceId());
        DoubleArrayListWritable newVal = message.getMessage();
        if (currVal == null || currVal.compareTo(newVal) != 0) {
          getValue().setNeighborValue(message.getSourceId(), newVal);
          isNeighUpdated = true;
        }
      } // END OF IF CLAUSE - delta caching is enabled

      // If delta caching is NOT enabled
      if (!isDeltaEnabled) {
        /* Calculate error */
        double observed = (double) getEdgeValue(message.getSourceId()).get();
        err = getError(getValue().getLatentVector(),
          message.getMessage(), observed);
        // Change the Vertex Latent Vector based on SGD equation
        runSgdAlgorithm(message.getMessage(), lambda, gamma, err);
        err = getError(getValue().getLatentVector(),
          message.getMessage(),
          observed);
        // If termination flag is set to RMSE OR RMSE aggregator is enabled
        if (factorFlag.equals("rmse") || rmseTolerance != 0f) {
          rmseErr += Math.pow(err, 2);
        }
      } // END OF IF CLAUSE - delta caching is NOT enabled
    } // END OF LOOP - for each message

    // If delta caching is enabled
    // Go through the edges and execute the SGD computation
    if (isDeltaEnabled) {
      // FOR LOOP - for each edge
      for (Entry<IntWritable, DoubleArrayListWritable> vvertex : getValue()
        .getAllNeighValue().entrySet()) {
        // Calculate error
        double observed = (double) getEdgeValue(vvertex.getKey()).get();
        err = getError(getValue().getLatentVector(),
          vvertex.getValue(),
          observed);
        // If at least one neighbor has changed its latent vector,
        // then calculation of vertex can not be avoided
        if (isNeighUpdated) {
          // Change the Vertex Latent Vector based on SGD equation
          runSgdAlgorithm(vvertex.getValue(), lambda, gamma, err);
          err = getError(getValue().getLatentVector(),
            vvertex.getValue(), observed);
        } // END OF IF CLAUSE - (neighUpdated)
        // If termination flag is set to RMSE or RMSE aggregator is true
        if (factorFlag.equals("rmse") || rmseTolerance != 0f) {
          rmseErr += Math.pow(err, 2);
        }
      }  // END OF LOOP - for each edge
    } // END OF IF CLAUSE - (isDeltaEnabled)

    // If halt factor is set to basic - set number of iterations + 1
    if (factorFlag.equals("basic")) {
      haltFactor = tolerance + 1;
    }
    // If RMSE aggregator flag is true - send rmseErr to aggregator
    if (rmseTolerance != 0f) {
      this.aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmseErr));
    }
    // If termination factor is set to RMSE - set the RMSE parameter
    if (factorFlag.equals("rmse")) {
      haltFactor = getRMSE(rmseErr);
    }
    // If termination factor is set to L2NOrm - set the L2NORM parameter
    if (factorFlag.equals("l2norm")) {
      haltFactor = getL2Norm(initialValue, getValue().getLatentVector());
    }
    if (getSuperstep() == 0 ||
      (haltFactor > tolerance && getSuperstep() < iterations)) {
      sendMessage();
    }
    // haltFactor is used in the OutputFormat file. --> To print the error
    if (factorFlag.equals("basic")) {
      haltFactor = err;
    }
    voteToHalt();
  } // END OF compute()

  /**
   * Return type of current vertex
   *
   * @return item
   */
  public boolean isItem() {
    return isItem;
  }

  /** Initialize Vertex Latent Vector
   * @param vectorSize Latent Vector Size
   */
  public void initLatentVector(int vectorSize) {
    DoubleArrayListHashMapWritable value =
      new DoubleArrayListHashMapWritable();
    for (int i = 0; i < vectorSize; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((double) (getId().get() + i) % 100d) / 100d));
    }
    setValue(value);
  }

  /**
   * Modify Vertex Latent Vector based on SGD equation
   *
   * @param vvertex Vertex value
   * @param vectorSize Latent Vector Size
   * @param lambda regularization parameter
   * @param gamma learning rate
   */
  public void runSgdAlgorithm(
    DoubleArrayListWritable vvertex, float lambda, float gamma, double err) {
    /**
     * vertex_vector = vertex_vector + part3
     *
     * part1 = LAMBDA * vertex_vector
     * part2 = real_value - dot_product(vertex_vector,other_vertex_vector)) *
     * other_vertex_vector
     * part3 = - GAMMA * (part1 + part2)
     */
	DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    DoubleArrayListWritable part3 = new DoubleArrayListWritable();
    DoubleArrayListWritable value = new DoubleArrayListWritable();
    part1 = numMatrixProduct((double) lambda,
      getValue().getLatentVector());
    part2 = numMatrixProduct((double) err, vvertex);
    part3 = numMatrixProduct((double) -gamma,
      dotAddition(part1, part2));
    value = dotAddition(getValue().getLatentVector(), part3);
    keepXdecimals(value, DECIMALS);
    getValue().setLatentVector(value);
    updatesNum++;
  }

  /**
   * Decimal Precision of latent vector values
   *
   * @param value Value to be truncated
   * @param x Number of decimals to keep
   */
  public void keepXdecimals(DoubleArrayListWritable value, int x) {
    double num = 1;
    for (int i = 0; i < x; i++) {
      num *= 10;
    }
    for (int i = 0; i < value.size(); i++) {
      value.set(i,
        new DoubleWritable(
          (double) (Math.round(value.get(i).get() * num) / num)));
    }
  }

  /*** Send messages to neighbours */
  public void sendMessage() {
    // Create a message and wrap together the source id and the message
    MessageWrapper message = new MessageWrapper();
    message.setSourceId(getId());

    if (getSuperstep() == 0) {
      for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
        DoubleArrayListWritable x = new DoubleArrayListWritable(getValue()
          .getLatentVector());
        x.add(new DoubleWritable(edge.getValue().get()));
        message.setMessage(x);
        sendMessage(edge.getTargetVertexId(), message);
      }
    } else {
      message.setMessage(getValue().getLatentVector());
      sendMessageToAllEdges(message);
    }
  }

  /**
   * Calculate the RMSE on the errors calculated by the current vertex
   *
   * @param msgCounter Count of messages received
   * @return RMSE result
   */
  public double getRMSE(double rmseErr) {
    return Math.sqrt(rmseErr / (double) messagesNum);
  }

  /**
   * Calculate the L2Norm on the initial and final value of vertex
   *
   * @param valOld Old value
   * @param valNew New value
   * @return result of L2Norm equation
   * */
  public double getL2Norm(DoubleArrayListWritable valOld,
    DoubleArrayListWritable valNew) {
    double result = 0;
    for (int i = 0; i < valOld.size(); i++) {
      result += Math.pow(valOld.get(i).get() - valNew.get(i).get(), 2);
    }
    return Math.sqrt(result);
  }

  /**
   * Calculate the error: e=observed-predicted
   *
   * @param vectorSize Latent Vector Size
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @param observed Observed value
   * @return Result from deducting observed value from predicted
   */
  public double getError(DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB, double observed) {
    double predicted = dotProduct(vectorA, vectorB);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);
    return predicted - observed;
  }

  /**
   * Calculate the dot product of 2 vectors: vector1*vector2
   *
   * @param vectorSize Latent Vector Size
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return Result from dot product of 2 vectors
   */
  public double dotProduct(DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB) {
    double result = 0d;
    for (int i = 0; i < vectorA.size(); i++) {
      result += vectorA.get(i).get() * vectorB.get(i).get();
    }
    return result;
  }

  /**
   * Calculate the dot addition of 2 vectors: vectorA+vectorB
   *
   * @param vectorSize Latent Vector Size
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return result Result from dot addition of the two vectors
   */
  public DoubleArrayListWritable dotAddition(
    DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < vectorA.size(); i++) {
      result.add(new DoubleWritable
        (vectorA.get(i).get() + vectorB.get(i).get()));
    }
    return result;
  }

  /**
   * Calculate the product num*matirx
   *
   * @param vectorSize Latent Vector Size
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   */
  public DoubleArrayListWritable numMatrixProduct(
    double num, DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < matrix.size(); i++) {
      result.add(new DoubleWritable(num * matrix.get(i).get()));
    }
    return result;
  }

  /**
   * Return amount of vertex updates
   *
   * @return updatesNum
   * */
  public int getUpdates() {
    return updatesNum;
  }

  /**
   * Return amount messages received
   *
   * @return messagesNum
   * */
  public int getMessages() {
    return messagesNum;
  }
  
  /**
   * Return amount of vertex updates
   *
   * @return haltFactor
   * */
  public double getHaltFactor() {
    return haltFactor;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute
  extends DefaultMasterCompute {
    @Override
    public void compute() {
      // Set the Convergence Tolerance
      float rmseTolerance = getContext().getConfiguration()
        .getFloat(RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
      double numRatings=0;
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

        System.out.println("SS:" + getSuperstep() + ", Total RMSE: " +
          totalRMSE + " = sqrt(" + getAggregatedValue(RMSE_AGGREGATOR) +
          " / " + numRatings + ")");
      }
      //if (totalRMSE < rmseTolerance) {
        //haltComputation();
      //}
    } // END OF compute()

    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
    }
  }
}
