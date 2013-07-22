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

public class Svd extends Vertex<IntWritable,
DoubleArrayListHashMapDoubleWritable, DoubleWritable, SvdMessageWrapper> {
  public static final String OVERALL_RATING_AGGREGATOR =
    "OVERALL_RATING_AGGREGATOR";
  /** Keyword for RMSE aggregator tolerance */
  public static final String RMSE_AGGREGATOR = "svd.rmse.aggregator";
  /** Default value for parameter enabling the RMSE aggregator */
  public static final float RMSE_AGGREGATOR_DEFAULT = 0f;
  /** Keyword for parameter choosing the halt factor */
  public static final String HALT_FACTOR = "svd.halt.factor";
  /** Default value for parameter choosing the halt factor */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Keyword for parameter setting the convergence tolerance parameter
   *  depending on the version enabled; l2norm or rmse */
  public static final String TOLERANCE_KEYWORD = "svd.halting.tolerance";
  /** Default value for TOLERANCE */
  public static final float TOLERANCE_DEFAULT = 1f;
  /** Keyword for parameter setting the number of iterations */
  public static final String ITERATIONS_KEYWORD = "svd.iterations";
  /** Default value for ITERATIONS */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Regularization parameter LAMBDA */
  public static final String LAMBDA_KEYWORD = "svd.lambda";
  /** Default value for LABDA */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the learning rate GAMMA */
  public static final String GAMMA_KEYWORD = "svd.gamma";
  /** Default value for GAMMA */
  public static final float GAMMA_DEFAULT = 0.005f;
  /** Keyword for parameter setting the Latent Vector Size */
  public static final String VECTOR_SIZE_KEYWORD = "svd.vector.size";
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
  DoubleArrayListWritable initialValue;
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
  public void compute(Iterable<SvdMessageWrapper> messages) {   
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

    // First superstep for users (superstep 0) & items (superstep 1)
    // Initialize vertex latent vector and baseline estimate
    if (getSuperstep() < 2) {
      initValue(vectorSize);
      // For L2Norm
      initialValue = new DoubleArrayListWritable(getValue().getLatentVector());
    }
    // Send sum of ratings to aggregator
    if (getSuperstep() == 0) {
      double sum=0;
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
    for (int i=0; i < vectorSize; i++) {
      relativeValuesSum.add(i, new DoubleWritable(0d));
    }
    // FOR LOOP - for each message
    for (SvdMessageWrapper message : messages) {
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
      } // END OF IF CLAUSE - superstep==1

      // Calculate error
      double observed = (double) getEdgeValue(message.getSourceId()).get();
      computeBaseline(lambda, gamma, err);
      // Change the Vertex Latent Vector based on SVD equation
      if (!isItem()){
        // Users - supersteps 0, 2, 4, 6, ...
        double predicted = 
          predictRating(message.getMessage(),
            message.getBaselineEstimate().get());
        err = observed - predicted;
        computeValue(lambda, gamma, err, message.getMessage());
        relativeValuesSum = dotAddition(relativeValuesSum,
          message.getRelativeValue());
      } else {
        // Items - supersteps 1, 3, 5, 7, ...
        double predicted = 
          predictRating(message.getMessage(),
            message.getBaselineEstimate().get(), message.getRelativeValue());
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
  public void initValue(int vectorSize) {
    DoubleArrayListHashMapDoubleWritable value =
      new DoubleArrayListHashMapDoubleWritable();
    // Initialize Latent Vector
    for (int i = 0; i < vectorSize; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((double) (getId().get() + i) % 100d) / 100d));
    }
    // Initialize Baseline Estimate
    value.setBaselineEstimate(new DoubleWritable(
      (getId().get() % 100d) / 100d));
    // Initialize Relative Value
    for (int i = 0; i < vectorSize; i++) {
      value.setRelativeValue(i, new DoubleWritable(
        ((double) (getId().get() + i) % 100d) / 100d));
    }
    setValue(value);
  }

  /**
   * Compute the baseline
   * b = b + gamma * (error - lambda * b)
   *
   * @param labmda Regularization parameter
   * @param gamma Learning rate
   * @param err error between predicted and actual rating
   */
  
  public void computeBaseline(float lambda, float gamma, double err) {
    getValue().setBaselineEstimate(new DoubleWritable(
      getValue().getBaselineEstimate().get() +
      gamma * ( err - lambda * getValue().getBaselineEstimate().get())));
  }

  /**
   * Compute Relative Value
   * y_j = y_j + gamma * (err * numAllEdges * q_i - lambda * y_i)
   *
   * @param labmda Regularization parameter
   * @param gamma Learning rate
   * @param err error between predicted and actual rating
   */
  
  public void computeRelativeValue(float lambda, float gamma, double err,
    DoubleArrayListWritable vvertex) {
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    DoubleArrayListWritable part3 = new DoubleArrayListWritable();

    part1 = numMatrixProduct(err * getNumEdges(), vvertex);
    part2 = numMatrixProduct(lambda, getValue().getRelativeValue());
    part3 = numMatrixProduct(gamma, dotSub(part1, part2));
    getValue().setRelativeValue(dotAddition(getValue().getRelativeValue(), part3));
  }

  /**
   * Compute the user's value
   * pu = pu + gamma * (error * qi - lamda * pu)
   *
   * @param labmda Regularization parameter
   * @param gamma Learning rate
   * @param err Error between predicted and actual rating
   * @param qi Other vertex latent vector
   */
  public void computeValue(float lambda, float gamma, double err,
    DoubleArrayListWritable vvertex) {
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
   * Compute the item's value
   * qi = qi + gamma * (error * (pu + numAllEdges * sum(y_j)) - lamda * qi)
   *
   * @param labmda Regularization parameter
   * @param gamma Learning rate
   * @param err Error between predicted and actual rating
   * @param qi Other vertex latent vector
   */
  public void computeValue(float lambda, float gamma, double err,
    DoubleArrayListWritable vvertex, DoubleArrayListWritable relativeValues) {

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
   * Compute Predicted Rating when vertex is a user
   *
   * r_ui = b_ui + dotProduct(q_i * (p_u + numEdges * sum(y_i)))
   * where b_ui = m + b_u + b_i
   * @param vvertex Neighbor's Latent Vector
   * @param otherBaselineEstimate
   */
  public double predictRating(DoubleArrayListWritable vvertex,
    double otherBaselineEstimate){
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();

    part1 = numMatrixProduct(getNumEdges(), getValue().getRelativeValue());
    part2 = dotAddition(getValue().getLatentVector(), part1);
    double part3 = dotProduct(vvertex, part2);
    double rating = ((DoubleWritable)
      getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() +
      getValue().getBaselineEstimate().get() + otherBaselineEstimate + part3; 
    return rating;
  }

  /**
   * Compute Predicted Rating when vertex is an item
   *
   * r_ui = b_ui + dotProduct(q_i * (p_u + numEdges * sum(y_i)))
   * where b_ui = m + b_u + b_i
   * @param vvertex Neighbor's Latent Vector
   * @param otherBaselineEstimate
   */
  public double predictRating(DoubleArrayListWritable vvertex,
    double otherBaselineEstimate, DoubleArrayListWritable relativeValues){
    DoubleArrayListWritable part1 = new DoubleArrayListWritable();
    DoubleArrayListWritable part2 = new DoubleArrayListWritable();
    part1 = numMatrixProduct(relativeValues.size(), relativeValues);
    part2 = dotAddition(vvertex, part1);
    double part3 = dotProduct(getValue().getLatentVector(), part2);
    double rating = ((DoubleWritable)
      getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() +
      getValue().getBaselineEstimate().get() + otherBaselineEstimate + part3;
    return rating;
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
    SvdMessageWrapper message = new SvdMessageWrapper();
    message.setSourceId(getId());
    message.setBaselineEstimate(getValue().getBaselineEstimate());
    message.setRelativeValue(getValue().getRelativeValue());
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
  /*public double getError(DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB, double observed) {
    double predicted = dotProduct(vectorA, vectorB);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);
    return predicted - observed;
  }*/

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
   * Calculate the dot addition of 2 vectors: vectorA+vectorB
   *
   * @param vectorSize Latent Vector Size
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return result Result from dot addition of the two vectors
   */
  public DoubleArrayListWritable dotSub(
    DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < vectorA.size(); i++) {
      result.add(new DoubleWritable
        (vectorA.get(i).get() - vectorB.get(i).get()));
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
   * Calculate the product num*matirx
   *
   * @param vectorSize Latent Vector Size
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   */
  public DoubleArrayListWritable numMatrixAddition(
    double num, DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < matrix.size(); i++) {
      result.add(new DoubleWritable(num + matrix.get(i).get()));
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
   * Define whether the halt factor is "basic", "rmse" or "l2norm"
   *
   * @param factorFlag  Halt factor
   * @param msgCounter  Number of messages received
   * @param initialValue Vertex initial value
   *
   * @return factor number of halting barrier
   */
  public double defineFactor(String factorFlag,
      DoubleArrayListWritable initialValue, float tolerance, double rmseErr) {
    double factor = 0d;
    if (factorFlag.equals("basic")) {
      factor = tolerance + 1d;
    } else if (factorFlag.equals("rmse")) {
      factor = getRMSE(rmseErr);
    } else if (factorFlag.equals("l2norm")) {
      factor = getL2Norm(initialValue, getValue().getLatentVector());
    } else {
      throw new RuntimeException("BUG: halt factor " + factorFlag +
        " is not included in the recognized options");
    }
    return factor;
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
      double avgRatings = 0;

      if (getSuperstep() > 1) {
        // In superstep=1 only half edges are created (users to items)
        if (getSuperstep() == 2) {
          numRatings = getTotalNumEdges();
          avgRatings = ((DoubleWritable)
            getAggregatedValue(OVERALL_RATING_AGGREGATOR)).get() / numRatings;
          System.out.println("SS:" + getSuperstep() + ", avgRatings: " +
            avgRatings);
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
      if (totalRMSE < rmseTolerance) {
        haltComputation();
      }
    } // END OF compute()

    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
      registerPersistentAggregator(OVERALL_RATING_AGGREGATOR, DoubleSumAggregator.class);
    }
  }
}
