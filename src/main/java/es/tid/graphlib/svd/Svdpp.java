package es.tid.graphlib.svd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import es.tid.graphlib.utils.DoubleArrayListWritable;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "SVD++",
  description = "Minimizes the error in users preferences predictions")

public class Svdpp extends Vertex<IntWritable,
DoubleArrayListHashMapDoubleWritable, DoubleWritable, SvdMessageWrapper> {
  /** Name of aggregator that aggregates all ratings. */
  public static final String OVERALL_RATING_AGGREGATOR =
    "OVERALL_RATING_AGGREGATOR";
  /** Keyword for RMSE aggregator tolerance. */
  public static final String RMSE_AGGREGATOR = "svd.rmse.aggregator";
  /** Default value for parameter enabling the RMSE aggregator. */
  public static final float RMSE_AGGREGATOR_DEFAULT = 0f;
  /** Keyword for parameter choosing the halt factor. */
  public static final String HALT_FACTOR = "svd.halt.factor";
  /** Default value for parameter choosing the halt factor. */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Keyword for parameter setting the convergence tolerance parameter.
   *  depending on the version enabled; l2norm or rmse */
  public static final String TOLERANCE_KEYWORD = "svd.halting.tolerance";
  /** Default value for TOLERANCE. */
  public static final float TOLERANCE_DEFAULT = 1f;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS_KEYWORD = "svd.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the regularization parameter LAMBDA. */
  public static final String LAMBDA_KEYWORD = "svd.lambda";
  /** Default value for LABDA. */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the learning rate GAMMA. */
  public static final String GAMMA_KEYWORD = "svd.gamma";
  /** Default value for GAMMA. */
  public static final float GAMMA_DEFAULT = 0.005f;
  /** Keyword for parameter setting the Latent Vector Size. */
  public static final String VECTOR_SIZE_KEYWORD = "svd.vector.size";
  /** Default value for GAMMA. */
  public static final int VECTOR_SIZE_DEFAULT = 2;
  /** Max rating. */
  public static final double MAX = 5;
  /** Min rating. */
  public static final double MIN = 0;
  /** Decimals. */
  public static final int DECIMALS = 4;
  /** Number used in the initialization of values. */
  public static final double HUNDRED = 100;
  /** Number used in the keepXdecimals method. */
  public static final int TEN = 10;
  /** Factor Error: it may be RMSD or L2NORM on initial&final vector. */
  private double haltFactor = 0d;
  /** Number of updates - used in the Output Format. */
  private int updatesNum = 0;
  /** Type of vertex 0 for user, 1 for item - used in the Output Format. */
  private boolean isItem = false;
  /**
   * Initial vector value to be used for the L2Norm case.
   * Keep it outside the compute() method
   * value has to preserved throughout the supersteps
   */
  private DoubleArrayListWritable initialValue;
  /**
   * Counter of messages received.
   * This is different from getNumEdges() because a
   * neighbor may not send a message
   */
  private int messagesNum = 0;

  /**
   * Compute method.
   * @param messages Messages received
   */
  public final void compute(final Iterable<SvdMessageWrapper> messages) {
    /** Error between predicted and observed rating */
    double err = 0d;

    /* Flag for checking if parameter for RMSE aggregator received */
    float rmseTolerance = getContext().getConfiguration().getFloat(
      RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
    /*
     * Flag for checking which termination factor to use:
     * basic, rmse, l2norm
     */
    String factorFlag = getContext().getConfiguration().get(
      HALT_FACTOR, HALT_FACTOR_DEFAULT);
    /* Set the number of iterations */
    int iterations = getContext().getConfiguration().getInt(
      ITERATIONS_KEYWORD, ITERATIONS_DEFAULT);
    /* Set the Convergence Tolerance */
    float tolerance = getContext().getConfiguration().getFloat(
      TOLERANCE_KEYWORD, TOLERANCE_DEFAULT);
    /* Set the Regularization Parameter LAMBDA */
    float lambda = getContext().getConfiguration().getFloat(
      LAMBDA_KEYWORD, LAMBDA_DEFAULT);
    /* Set the Learning Rate GAMMA */
    float gamma = getContext().getConfiguration().getFloat(
      GAMMA_KEYWORD, GAMMA_DEFAULT);
    /* Set the size of the Latent Vector*/
    int vectorSize = getContext().getConfiguration().getInt(
      VECTOR_SIZE_KEYWORD, VECTOR_SIZE_DEFAULT);

    // First superstep for users (superstep 0) & items (superstep 1)
    // Initialize vertex latent vector and baseline estimate
    if (getSuperstep() < 2) {
      initValue(vectorSize);
      // For L2Norm
      initialValue = new DoubleArrayListWritable(getValue().getLatentVector());
    }
    // Send sum of ratings to aggregator
    if (getSuperstep() == 0) {
      double sum = 0;
      for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
        sum += edge.getValue().get();
      }
      this.aggregate(OVERALL_RATING_AGGREGATOR, new DoubleWritable(sum));
    }
    // Set flag for items - used in the Output Format
    if (getSuperstep() == 1) {
      isItem = true;
    }

    // Used if RMSE version or RMSE aggregator is enabled
    double rmseErr = 0d;

    // Used for user - to calculate the sum of y_i
    DoubleArrayListWritable relativeValuesSum = new DoubleArrayListWritable();
    for (int i = 0; i < vectorSize; i++) {
      relativeValuesSum.add(i, new DoubleWritable(0d));
    }
    // FOR LOOP - for each message
    for (SvdMessageWrapper message : messages) {
      messagesNum++;
      // First superstep for items:
      // 1. Create outgoing edges of items
      // 2. Store the rating given from users in the outgoing edges
      if (getSuperstep() == 1) {
        double observed = message.getMessage().get(
          message.getMessage().size() - 1).get();
        DefaultEdge<IntWritable, DoubleWritable> edge =
          new DefaultEdge<IntWritable, DoubleWritable>();
        edge.setTargetVertexId(message.getSourceId());
        edge.setValue(new DoubleWritable(observed));
        addEdge(edge);
        // Remove the last value from message
        // It's there only for the 1st round of items
        message.getMessage().remove(message.getMessage().size() - 1);
      } // END OF IF CLAUSE - superstep==1

      // Calculate error
      double observed = (double) getEdgeValue(message.getSourceId()).get();
      computeBaseline(lambda, gamma, err);
      // Change the Vertex Latent Vector based on SVD equation
      if (!isItem()) {
        // Users - supersteps 0, 2, 4, 6, ...
        double predicted =
          predictRating(message.getMessage(),
            message.getBaselineEstimate().get());
        System.out.println("SS:" + getSuperstep() + ", predicted: " + predicted);
        err = observed - predicted;
        computeValue(lambda, gamma, err, message.getMessage());
        relativeValuesSum = dotAddition(relativeValuesSum,
          message.getRelativeValue());
      } else {
        // Items - supersteps 1, 3, 5, 7, ...
        double predicted =
          predictRating(message.getMessage(),
            message.getBaselineEstimate().get(), message.getRelativeValue(),
            message.getNumEdges());
        System.out.println("SS:" + getSuperstep() + ", predicted: " + predicted);
        err = observed - predicted;
        computeValue(lambda, gamma, err, message.getMessage(),
          message.getRelativeValue());
        computeRelativeValue(lambda, gamma, err, message.getMessage());
      }
      // If termination flag is set to RMSE OR RMSE aggregator is enabled
      if (factorFlag.equals("rmse") || rmseTolerance != 0f) {
        rmseErr += Math.pow(err, 2);
      }
    } // END OF LOOP - for each message

    if (getSuperstep() > 1 && !isItem()) {
      getValue().setRelativeValue(relativeValuesSum);
    }
    haltFactor =
      defineFactor(factorFlag, initialValue, tolerance, rmseErr);

    // If RMSE aggregator flag is true - send rmseErr to aggregator
    if (rmseTolerance != 0f) {
      this.aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmseErr));
    }

    if (getSuperstep() == 0
      || (haltFactor > tolerance && getSuperstep() < iterations)) {
      sendMessage();
    }
    // haltFactor is used in the OutputFormat file. --> To print the error
    if (factorFlag.equals("basic")) {
      haltFactor = err;
    }
    voteToHalt();
  } // END OF compute()

  /**
   * Return type of current vertex.
   *
   * @return item
   */
  public final boolean isItem() {
    return isItem;
  }

  /** Initialize Vertex Latent Vector.
   *
   * @param vectorSize Latent Vector Size
   */
  public final void initValue(final int vectorSize) {
    DoubleArrayListHashMapDoubleWritable value =
      new DoubleArrayListHashMapDoubleWritable();
    // Initialize Latent Vector
    for (int i = 0; i < vectorSize; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((double) (getId().get() + i) % HUNDRED) / HUNDRED));
    }
    // Initialize Baseline Estimate
    value.setBaselineEstimate(new DoubleWritable(
      (getId().get() % HUNDRED) / HUNDRED));
    // Initialize Relative Value
    for (int i = 0; i < vectorSize; i++) {
      value.setRelativeValue(i, new DoubleWritable(
        ((double) (getId().get() + i) % HUNDRED) / HUNDRED));
    }
    setValue(value);
  }

  /**
   * Compute the baseline.
   * b = b + gamma * (error - lambda * b)
   *
   * @param lambda Regularization parameter
   * @param gamma Learning rate
   * @param err error between predicted and actual rating
   */

  public final void computeBaseline(final float lambda, final float gamma,
    final double err) {
    getValue().setBaselineEstimate(new DoubleWritable(
      getValue().getBaselineEstimate().get()
      + gamma * (err - lambda * getValue().getBaselineEstimate().get())));
  }

  /**
   * Compute Relative Value.
   * y_j = y_j + gamma * (err * numAllEdges * q_i - lambda * y_i)
   *
   * @param lambda Regularization parameter
   * @param gamma Learning rate
   * @param err error between predicted and actual rating
   * @param vvertex Other vertex latent vector
   */

  public final void computeRelativeValue(final float lambda,
    final float gamma, final double err,
    final DoubleArrayListWritable vvertex) {
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    DoubleArrayListWritable part3 = new DoubleArrayListWritable();

    part1 = numMatrixProduct(err * getNumEdges(), vvertex);
    part2 = numMatrixProduct(lambda, getValue().getRelativeValue());
    part3 = numMatrixProduct(gamma, dotSub(part1, part2));
    getValue().setRelativeValue(dotAddition(
      getValue().getRelativeValue(), part3));
  }

  /**
   * Compute the user's value.
   * pu = pu + gamma * (error * qi - lamda * pu)
   *
   * @param lambda Regularization parameter
   * @param gamma Learning rate
   * @param err Error between predicted and actual rating
   * @param vvertex Other vertex latent vector
   */
  public final void computeValue(final float lambda, final float gamma,
    final double err, final DoubleArrayListWritable vvertex) {
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    DoubleArrayListWritable part3 = new DoubleArrayListWritable();
    DoubleArrayListWritable value = new DoubleArrayListWritable();
    part1 = numMatrixProduct((double) err, vvertex);
    part2 = numMatrixProduct((double) lambda,
      getValue().getLatentVector());
    part3 = numMatrixProduct((double) gamma,
      dotSub(part1, part2));
    value = dotAddition(getValue().getLatentVector(), part3);
    getValue().setLatentVector(value);
    updatesNum++;
  }

  /**
   * Compute the item's value.
   * qi = qi + gamma * (error * (pu + numAllEdges * sum(y_j)) - lamda * qi)
   *
   * @param lambda Regularization parameter
   * @param gamma Learning rate
   * @param err Error between predicted and actual rating
   * @param vvertex Other vertex latent vector
   * @param relativeValues Relative Values
   */
  public final void computeValue(final float lambda, final float gamma,
    final double err, final DoubleArrayListWritable vvertex,
    final DoubleArrayListWritable relativeValues) {

    DoubleArrayListWritable part1a = new DoubleArrayListWritable();
    DoubleArrayListWritable part1b = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    DoubleArrayListWritable part3 = new DoubleArrayListWritable();
    DoubleArrayListWritable value = new DoubleArrayListWritable();

    part1a = dotAddition(vvertex,
      numMatrixProduct(relativeValues.size(), relativeValues));
    part1b = numMatrixProduct((double) err, part1a);
    part2 = numMatrixProduct((double) lambda,
      getValue().getLatentVector());
    part3 = numMatrixProduct((double) gamma,
      dotSub(part1b, part2));
    value = dotAddition(getValue().getLatentVector(), part3);
    getValue().setLatentVector(value);
    updatesNum++;
  }

  /**
   * Compute Predicted Rating when vertex is a user.
   *
   * r_ui = b_ui + dotProduct(q_i * (p_u + numEdges * sum(y_i)))
   * where b_ui = m + b_u + b_i
   *
   * @param vvertex Neighbor's Latent Vector
   * @param otherBaselineEstimate Item's Baseline Estimate
   *
   * @return rating Predicted rating
   */
  public final double predictRating(final DoubleArrayListWritable vvertex,
    final double otherBaselineEstimate) {
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();

    part1 = numMatrixProduct(getNumEdges(), getValue().getRelativeValue());
    part2 = dotAddition(getValue().getLatentVector(), part1);
    double part3 = dotProduct(vvertex, part2);
    double numEdges = 0d;
    if (getSuperstep() < 2) {
      numEdges = getTotalNumEdges();
    } else {
      numEdges = getTotalNumEdges() / (double) 2;
    }
    double avgRatings = ((DoubleWritable)
      getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() / numEdges;
    /*System.out.println("--avgRatings= " + avgRatings + ", total:" +
      ((DoubleWritable)
        getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() + ", edges: " +
      numEdges);*/
    double rating = avgRatings + getValue().getBaselineEstimate().get()
      + otherBaselineEstimate + part3;
    return rating;
  }

  /**
   * Compute Predicted Rating when vertex is an item.
   *
   * r_ui = b_ui + dotProduct(q_i * (p_u + numEdges * sum(y_i)))
   * where b_ui = m + b_u + b_i
   *
   * @param vvertex Neighbor's Latent Vector
   * @param otherBaselineEstimate User's Baseline Estimate
   * @param relativeValues Relative Values
   * @param numUserEdges Number of neighbors of user rated this item
   *
   * @return rating Predicted rating
   */
  public final double predictRating(final DoubleArrayListWritable vvertex,
    final double otherBaselineEstimate,
    final DoubleArrayListWritable relativeValues,
    final IntWritable numUserEdges) {
    DoubleArrayListWritable part1;
    DoubleArrayListWritable part2;

    part1 = numMatrixProduct((double) numUserEdges.get(), relativeValues);
    part2 = dotAddition(vvertex, part1);
    double part3 = dotProduct(getValue().getLatentVector(), part2);
    double numEdges = 0d;
    if (getSuperstep() < 2) {
      numEdges = getTotalNumEdges();
    } else {
      numEdges = getTotalNumEdges() / (double) 2;
    }
    double avgRatings = ((DoubleWritable)
      getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() / numEdges;
    double rating = avgRatings + getValue().getBaselineEstimate().get()
      + otherBaselineEstimate + part3;
    return rating;
  }
  /**
   * Decimal Precision of latent vector values.
   *
   * @param value Value to be truncated
   * @param x Number of decimals to keep
   */
  public final void keepXdecimals(final DoubleArrayListWritable value,
    final int x) {
    for (int i = 0; i < value.size(); i++) {
      value.set(i,
        new DoubleWritable(
          (double) (Math.round(value.get(i).get() * Math.pow(TEN, x - 1))
            / Math.pow(TEN, x - 1))));
    }
  }

  /**
   * Send messages to neighbors.
   */
  public final void sendMessage() {
    // Create a message and wrap together the source id and the message
    SvdMessageWrapper message = new SvdMessageWrapper();
    message.setSourceId(getId());
    message.setBaselineEstimate(getValue().getBaselineEstimate());
    message.setRelativeValue(getValue().getRelativeValue());
    if (!isItem()) {
      message.setNumEdges(new IntWritable(getNumEdges()));
    } else {
      message.setNumEdges(new IntWritable(0));
    }
    // At superstep 0, users send rating to items
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
   * Calculate the RMSE on the errors calculated by the current vertex.
   *
   * @param rmseErr RMSE error
   * @return RMSE result
   */
  public final double getRMSE(final double rmseErr) {
    return Math.sqrt(rmseErr / (double) messagesNum);
  }

  /**
   * Calculate the L2Norm on the initial and final value of vertex.
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
   * Calculate the dot product of 2 vectors: vectorA * vectorB.
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
   * Calculate the dot addition of 2 vectors: vectorA + vectorB.
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return result Result from dot addition of the two vectors
   */
  public final DoubleArrayListWritable dotSub(
    final DoubleArrayListWritable vectorA,
    final DoubleArrayListWritable vectorB) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < vectorA.size(); i++) {
      result.add(new DoubleWritable(
        vectorA.get(i).get() - vectorB.get(i).get()));
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

  /**
   * Calculate the product num * matirx.
   *
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   */
  public final DoubleArrayListWritable numMatrixAddition(
    final double num, final DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < matrix.size(); i++) {
      result.add(new DoubleWritable(num + matrix.get(i).get()));
    }
    return result;
  }

  /**
   * Return amount of vertex updates.
   *
   * @return updatesNum
   * */
  public final int getUpdates() {
    return updatesNum;
  }

  /**
   * Return amount messages received.
   *
   * @return messagesNum
   * */
  public final int getMessages() {
    return messagesNum;
  }

  /**
   * Return amount of vertex updates.
   *
   * @return haltFactor
   * */
  public final double getHaltFactor() {
    return haltFactor;
  }

  /**
   * Define whether the halt factor is "basic", "rmse" or "l2norm".
   *
   * @param factorFlag  Halt factor
   * @param pInitialValue Vertex initial value
   * @param pTolerance Tolerance
   * @param rmseErr RMSE error
   *
   * @return factor number of halting barrier
   */
  public final double defineFactor(final String factorFlag,
      final DoubleArrayListWritable pInitialValue, final float pTolerance,
      final double rmseErr) {
    double factor = 0d;
    if (factorFlag.equals("basic")) {
      factor = pTolerance + 1d;
    } else if (factorFlag.equals("rmse")) {
      factor = getRMSE(rmseErr);
    } else if (factorFlag.equals("l2norm")) {
      factor = getL2Norm(pInitialValue, getValue().getLatentVector());
    } else {
      throw new RuntimeException("BUG: halt factor " + factorFlag
        + " is not included in the recognized options");
    }
    return factor;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute
  extends DefaultMasterCompute {
    @Override
    public final void compute() {
      // Set the Convergence Tolerance
      float rmseTolerance = getContext().getConfiguration()
        .getFloat(RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
      double numRatings = 0d;
      double totalRMSE = 0d;
      double totalRatings = 0d;

      if (getSuperstep() > 1) {
        // In superstep=1 only half edges are created (users to items)
        if (getSuperstep() == 2) {
          numRatings = getTotalNumEdges();
          totalRatings = ((DoubleWritable)
            getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get();
          System.out.println("SS:" + getSuperstep() + ", totalRatings: "
            + totalRatings);
        } else {
          numRatings = getTotalNumEdges() / 2;
        }
      }
      if (rmseTolerance != 0f) {
        totalRMSE = Math.sqrt(((DoubleWritable)
          getAggregatedValue(RMSE_AGGREGATOR)).get() / numRatings);

        System.out.println("SS:" + getSuperstep() + ", Total RMSE: "
        + totalRMSE + " = sqrt(" + getAggregatedValue(RMSE_AGGREGATOR)
        + " / " + numRatings + ")");
      }
      if (totalRMSE < rmseTolerance) {
        haltComputation();
      }
    } // END OF compute()

    @Override
    public final void initialize() throws InstantiationException,
    IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
      registerPersistentAggregator(OVERALL_RATING_AGGREGATOR,
       DoubleSumAggregator.class);
    }
  }
}
