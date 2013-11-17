package ml.grafos.okapi.cf.svd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.jblas.FloatMatrix;


/**
 * Singular Value Decomposition (SVD) implementation.
 */
@Algorithm(
  name = "SVD++",
  description = "Minimizes the error in users' preferences predictions")
public class Svdpp extends BasicComputation<CfLongId,
  FloatMatrixWritable, FloatWritable, SvdppMessageWrapper> 
{
  /** Name of aggregator that aggregates all ratings. */
  public static final String OVERALL_RATING_AGGREGATOR =
    "svd.overall.rating.aggregator";
  /** Keyword for RMSE target */
  public static final String RMSE_TARGET = "svd.rmse.target";
  /** Default value for parameter enabling the RMSE aggregator. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
  /** Keyword for parameter setting the update tolerance parameter. */
  public static final String TOLERANCE = "svd.tolerance";
  /** Default value for TOLERANCE. */
  public static final float TOLERANCE_DEFAULT = -1f;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS = "svd.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** User regularization parameter. */
  public static final String LAMBDA_USER = "svd.lambda.user";
  /** Default value for user regularization parameter */
  public static final float LAMBDA_USER_DEFAULT = 0.01f;
  /** User learning rate parameter */
  public static final String GAMMA_USER = "svd.gamma.user";
  /** Default value for user learning rate */
  public static final float GAMMA_USER_DEFAULT = 0.005f;
   /** Item regularization parameter. */
  public static final String LAMBDA_ITEM = "svd.lambda.item";
  /** Default value for item regularization parameter */
  public static final float LAMBDA_ITEM_DEFAULT = 0.01f;
  /** Item learning rate parameter */
  public static final String GAMMA_ITEM = "svd.gamma.item";
  /** Default value for item learning rate */
  public static final float GAMMA_ITEM_DEFAULT = 0.005f;
  /** Max rating. */
  public static final String MAX_RATING = "svd.max.rating";
  /** Default maximum rating */
  public static final float MAX_RATING_DEFAULT = 5.0f;
  /** Min rating. */
  public static final String MIN_RATING = "svd.min.rating";
  /** Default minimum rating */
  public static final float MIN_RATING_DEFAULT = 0.0f;
  /** Latent vector size. */
  public static final String VECTOR_SIZE = "svd.vector.size";
  /** Default latent vector size */
  public static final int VECTOR_SIZE_DEFAULT = 50;
  
  /** Aggregator for the computation of RMSE */
  public static final String RMSE_AGGREGATOR = "svd.rmse.aggregator";
  
  public static final int VALUE_INDEX = 0;
  public static final int RELATIVE_VALUE_INDEX = 1;
  

  /**
   * Main SVD compute method.
   * 
   * @param messages
   *          Messages received
   */
  public final void compute(Vertex<CfLongId, FloatMatrixWritable, 
      FloatWritable> vertex, final Iterable<SvdppMessageWrapper> messages) {
    /** Error between predicted and observed rating */
    double err = 0d;

    // Used if RMSE version or RMSE aggregator is enabled
    double rmseErr = 0d;

 // Sum of items relative values - Used in user
    DoubleArrayListWritable relativeValuesSum = new DoubleArrayListWritable();
//    if (!isItem()) {
//      for (int i = 0; i < vectorSize; i++) {
//        relativeValuesSum.add(i, new DoubleWritable(0d));
//      }
//      if (getSuperstep() > 1) {
//        for (SvdppMessageWrapper message : messages) {
//          relativeValuesSum = dotAddition(relativeValuesSum,
//            message.getRelativeValue());
//        }
//        vertex.getValue().setRelativeValue(relativeValuesSum);
//      }
//    }
//    // FOR LOOP - for each message
    for (SvdppMessageWrapper message : messages) {

      // Calculate error
//      double observed = 
//          (double)vertex.getEdgeValue(message.getSourceId()).get();
//      computeBaseline(vertex, lambda, gamma, err);
//      // Change the Vertex Latent Vector based on SVD equation
//      if (!isItem()) {
//        // Users - supersteps 0, 2, 4, 6, ...
//        double predicted =
//          predictRating(vertex, message.getMessage(),
//            message.getBaselineEstimate().get());
//        err = observed - predicted;
//        computeValue(vertex, lambda, gamma, err, message.getMessage());
//      } else {
//        // Items - supersteps 1, 3, 5, 7, ...
//        double predicted =
//          predictRating(vertex, message.getMessage(),
//            message.getBaselineEstimate().get(), message.getRelativeValue(),
//            message.getNumEdges());
//        err = observed - predicted;
//        computeValue(vertex, lambda, gamma, err, message.getMessage(),
//          message.getRelativeValue(), message.getNumEdges());
//        computeRelativeValue(vertex, lambda, gamma, err, message.getMessage());
//      }
//      
      rmseErr += Math.pow(err, 2);
    } // END OF LOOP - for each message

//    haltFactor =
//      defineFactor(vertex, factorFlag, initialValue, tolerance, rmseErr);

    // If RMSE aggregator flag is true - send rmseErr to aggregator
    aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmseErr));

//    if (getSuperstep() == 0
//      || (haltFactor > tolerance && getSuperstep() < iterations)) {
//      sendMessage(vertex);
//    }
    
    vertex.voteToHalt();
  } // END OF compute()

//  /**
//   * Initialize Vertex Latent Vector.
//   * 
//   * @param vectorSize
//   *          Latent Vector Size
//   */
//  public final void initValue(
//      Vertex<Text, SvdppVertexValue, DoubleWritable> vertex, 
//      final int vectorSize) {
//    
//    SvdppVertexValue value = new SvdppVertexValue();
//    // Initialize Latent Vector
//    for (int i = 0; i < vectorSize; i++) {
//      value.setLatentVector(i, new DoubleWritable(
//        ((Double.parseDouble(
//          vertex.getId().toString().substring(2)) + i) % HUNDRED) / HUNDRED));
//    }
//    // Initialize Baseline Estimate
//    value.setBaselineEstimate(new DoubleWritable(
//      (Double.parseDouble(
//        vertex.getId().toString().substring(2)) % HUNDRED) / HUNDRED));
//    // Initialize Relative Value
//    for (int i = 0; i < vectorSize; i++) {
//      value.setRelativeValue(i, new DoubleWritable(
//        ((Double.parseDouble(
//          vertex.getId().toString().substring(2)) + i) % HUNDRED) / HUNDRED));
//    }
//    vertex.setValue(value);
//  }

  /**
   * Computes the predicted rating r between a user and an item based on the
   * formula:
   * r = b + q^T * (p + (1/sqrt(N) * sum(y_i)))
   * 
   * where
   * b: the baseline estimate of the user for the item
   * q: the item vector
   * p: the user vector
   * N: number of ratings of the user
   * y_i: the weight vector
   * 
   * @param baseline
   * @param user
   * @param item
   * @param numRatings
   * @param sumWeights
   * @param maxRating
   * @param minRating
   * @return
   */
  protected static final float computePredictedRating(final float meanRating, 
      final float userBaseline, final float itemBaseline, FloatMatrix user, 
      FloatMatrix item, final int numRatings, FloatMatrix sumWeights, 
      final float minRating, final float maxRating ) {
    
    float predicted = meanRating + userBaseline+ itemBaseline +
        item.dot(user.add(sumWeights.mul(1.0f/(float)(Math.sqrt(numRatings)))));
    
    // Correct the predicted rating to be between the min and max ratings
    predicted = Math.min(predicted, maxRating);
    predicted = Math.max(predicted, minRating);
    
    return predicted;
  }
  
  /**
   * Computes the updated baseline vector b based on the formula:
   * 
   * b := b + gamma * (error - lambda * b)
   * 
   * @param baseline
   * @param predictedRating
   * @param observedRating
   * @param gamma
   * @param lambda
   */
  protected static final float computeUpdatedBaseLine(float baseline, 
      final float predictedRating, final float observedRating, 
      final float gamma, final float lambda) {
    
    return baseline + 
        gamma*((predictedRating-observedRating)-lambda*baseline);
  }
  
//  /**
//   * Send messages to neighbors.
//   */
//  public final void sendMessage(
//      Vertex<Text, SvdppVertexValue, DoubleWritable> vertex) {
//    // Create a message and wrap together the source id and the message
//    SvdppMessageWrapper message = new SvdppMessageWrapper();
//    message.setSourceId(vertex.getId());
//    message.setBaselineEstimate(vertex.getValue().getBaselineEstimate());
//    message.setRelativeValue(vertex.getValue().getRelativeValue());
//    if (!isItem()) {
//      message.setNumEdges(new IntWritable(vertex.getNumEdges()));
//    } else {
//      message.setNumEdges(new IntWritable(0));
//    }
//    // At superstep 0, users send rating to items
//    if (getSuperstep() == 0) {
//      for (Edge<Text, DoubleWritable> edge : vertex.getEdges()) {
//        DoubleArrayListWritable x = 
//            new DoubleArrayListWritable(vertex.getValue().getLatentVector());
//        x.add(new DoubleWritable(edge.getValue().get()));
//        message.setMessage(x);
//        sendMessage(edge.getTargetVertexId(), message);
//      }
//    } else {
//      message.setMessage(vertex.getValue().getLatentVector());
//      sendMessageToAllEdges(vertex, message);
//    }
//  }

  /**
   * A value in the Svdpp algorithm consists of (i) the baseline estimate, (ii)
   * the latent vector, and (iii) the weight vector.
   * 
   * @author dl
   *
   */
  public static class SvdppValue implements Writable {
    private float baseline;
    private FloatMatrixWritable factors;
    private FloatMatrixWritable weight;

    public SvdppValue() {}
 
    public float getBaseline() { return baseline; }
    public void setBaseline(float baseline) { this.baseline = baseline; }
    public FloatMatrixWritable getFactors() { return factors; }
    public FloatMatrixWritable getWeight() { return weight; }

    public SvdppValue(float baseline, FloatMatrixWritable factors, 
        FloatMatrixWritable weight) {
      this.baseline = baseline;
      this.factors = factors;
      this.weight = weight;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      baseline = input.readFloat();
      factors = new FloatMatrixWritable();
      factors.readFields(input);
      weight = new FloatMatrixWritable();
      weight.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeFloat(baseline);
      factors.write(output);
      weight.write(output);
    }
  }

  /**
   * This computation class is used to initialize the factors of the user nodes
   * in the very first superstep, and send the first updates to the item nodes.
   * @author dl
   *
   */
  public static class InitUsersComputation extends BasicComputation<CfLongId, 
  SvdppValue, FloatWritable, FloatMatrixMessage> {

    @Override
    public void compute(Vertex<CfLongId, SvdppValue, 
        FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) 
            throws IOException {
      
      // Aggregate ratings. Necessary to compute the mean rating.
      double sum = 0;
      for (Edge<CfLongId, FloatWritable> edge : vertex.getEdges()) {
        sum += edge.getValue().get();
      }
      aggregate(OVERALL_RATING_AGGREGATOR, new DoubleWritable(sum));
      
      // Initialize the baseline estimate and the factor vector.
      
      int vectorSize = getContext().getConfiguration().getInt(
              VECTOR_SIZE, VECTOR_SIZE_DEFAULT);

      FloatMatrixWritable factors = new FloatMatrixWritable(1, vectorSize);
      
      Random randGen = new Random();
      for (int i=0; i<factors.length; i++) {
        factors.put(i, 0.01f*randGen.nextFloat());
      }
      
      float baseline = randGen.nextFloat();

      vertex.setValue(new SvdppValue(baseline, factors, 
          new FloatMatrixWritable())); // The weights vector is empty for users
      
      // Send ratings to all items so that they can create the reverse edges.
      for (Edge<CfLongId, FloatWritable> edge : vertex.getEdges()) {
        FloatMatrixMessage msg = new FloatMatrixMessage(vertex.getId(), 
            new FloatMatrixWritable(), // the matrix of this message is empty
            edge.getValue().get());    // because we only need the rating
        sendMessage(edge.getTargetVertexId(), msg);
      }

      vertex.voteToHalt();
    }
  }
  
  public static class InitItemsComputation extends BasicComputation<CfLongId, 
  SvdppValue, FloatWritable, FloatMatrixMessage> {

    @Override
    public void compute(
        Vertex<CfLongId, SvdppValue, FloatWritable> vertex,
        Iterable<FloatMatrixMessage> messages) throws IOException {
      
      // Create the reverse edges
      for (FloatMatrixMessage msg : messages) {
        DefaultEdge<CfLongId, FloatWritable> edge = 
            new DefaultEdge<CfLongId, FloatWritable>();
        edge.setTargetVertexId(msg.getSenderId());
        edge.setValue(new FloatWritable(msg.getScore()));
        vertex.addEdge(edge);
      }
      
      // Initialize baseline estimate and the factor and weight vectors

      int vectorSize = getContext().getConfiguration().getInt(
          VECTOR_SIZE, VECTOR_SIZE_DEFAULT);

      FloatMatrixWritable factors = new FloatMatrixWritable(1, vectorSize);
      FloatMatrixWritable weight = new FloatMatrixWritable(1, vectorSize);

      Random randGen = new Random();
      for (int i=0; i<factors.length; i++) {
        factors.put(i, 0.01f*randGen.nextFloat());
        weight.put(i, 0.01f*randGen.nextFloat());
      }
      float baseline = randGen.nextFloat();

      vertex.setValue(new SvdppValue(baseline, factors, weight));
      
      // Start iterations by sending vectors to users
      FloatMatrixWritable packedVectors = 
          new FloatMatrixWritable(2, vectorSize);
      packedVectors.putRow(0, factors);
      packedVectors.putRow(1, weight);

      sendMessageToAllEdges(vertex, 
          new FloatMatrixMessage(vertex.getId(), packedVectors, baseline));

      vertex.voteToHalt();
    }
  }
  
  public static class UserComputation extends BasicComputation<CfLongId, 
  SvdppValue, FloatWritable, FloatMatrixMessage> {

    private float lambda;
    private float gamma;
    private float minRating;
    private float maxRating;
    private int vectorSize;
    private float meanRating;
    
    protected void updateValue(FloatMatrix user, FloatMatrix item, 
        final float error, final float gamma, final float lambda) {
      
      user.addi(user.mul(-lambda*gamma).addi(item.mul(error*gamma)));
    }
    
    @Override
    public void preSuperstep() {
      lambda = getContext().getConfiguration().getFloat(LAMBDA_USER, 
          LAMBDA_USER_DEFAULT);
      gamma = getContext().getConfiguration().getFloat(GAMMA_USER, 
          GAMMA_USER_DEFAULT);
      minRating = getContext().getConfiguration().getFloat(MIN_RATING, 
          MIN_RATING_DEFAULT);
      maxRating = getContext().getConfiguration().getFloat(MAX_RATING, 
          MAX_RATING_DEFAULT);
      vectorSize = getContext().getConfiguration().getInt(VECTOR_SIZE, 
          VECTOR_SIZE_DEFAULT);
      meanRating = (float) (((DoubleWritable)getAggregatedValue(
          OVERALL_RATING_AGGREGATOR)).get()/getTotalNumEdges());
    }
    
    @Override
    public void compute(
        Vertex<CfLongId, SvdppValue, FloatWritable> vertex,
        Iterable<FloatMatrixMessage> messages) throws IOException {
      
      float userBaseline = vertex.getValue().getBaseline();
      
      FloatMatrix sumWeights = new FloatMatrix(1,vectorSize);
      for (FloatMatrixMessage msg : messages) {
        // The weights are in the second row of the matrix
        sumWeights.addi(msg.getFactors().getRow(1));
      }
      
      for (FloatMatrixMessage msg : messages) {
         // row 1 of the matrix in the message holds the item factors
        FloatMatrix itemFactors = msg.getFactors().getRow(0);
        // score holds the item baseline estimate
        float itemBaseline = msg.getScore();

        float observed = vertex.getEdgeValue(msg.getSenderId()).get();
        float predicted = computePredictedRating(
            meanRating, userBaseline, itemBaseline,
            vertex.getValue().getFactors(), itemFactors,
            vertex.getNumEdges(), sumWeights, minRating, maxRating);
        float error = predicted - observed;
        
        // Update baseline
        userBaseline = computeUpdatedBaseLine(userBaseline, predicted, 
            observed, gamma, lambda);
        
        // Update the value
        updateValue(vertex.getValue().getFactors(), itemFactors, error, 
            gamma, lambda);
      }
      
      vertex.voteToHalt();
    }
  }
  
  public static class ItemComputation extends BasicComputation<CfLongId, 
  FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {
    
    private float lambda;
    private float gamma;

    /**
     * Updates the item weight y based on the formula:
     * 
     * y_j = y_j + gamma * (err * (1/sqrt(N) * q_i - lambda * y_j)
     * 
     * where
     * y: the item weight
     * N: the number ratings
     * 
     * @param weight
     * @param item
     * @param error
     * @param numRatings
     * @param gamma
     * @param lambda
     */
    protected void updateWeight(FloatMatrix weight, FloatMatrix item, 
        final float error, final int numRatings, final float gamma, 
        final float lambda) {
      
      weight.addi(weight.mul(-lambda*gamma).addi(
          item.mul(error*gamma/(float)Math.sqrt(numRatings))));
    }
    
    /**
     * Updates the item vector q based on the formula:
     * q = q + gamma * (error * (p + (1/sqrt(N) * sum(y_j)) - lambda * q))
     * 
     * where,
     * q: the item vector
     * p: the user vector
     * N: the number of ratings of the user
     * 
     * @param item
     * @param user
     * @param sumWeights
     * @param error
     * @param numRatings
     * @param gamma
     * @param lambda
     */
    protected void updateValue(FloatMatrix item, FloatMatrix user, 
        FloatMatrix sumWeights, final float error, int numRatings, 
        final float gamma, final float lambda) {
      
      item.addi(user.mul(gamma*error).addi(item.mul(-lambda*gamma).addi(
          sumWeights.mul(gamma*error/(float)(Math.sqrt(numRatings))))));
    }
    
    @Override
    public void preSuperstep() {
      lambda = getContext().getConfiguration().getFloat(Svdpp.LAMBDA_ITEM, 
          Svdpp.LAMBDA_ITEM_DEFAULT);
      gamma = getContext().getConfiguration().getFloat(Svdpp.GAMMA_ITEM, 
          Svdpp.GAMMA_ITEM_DEFAULT);  
    }
    
    @Override
    public void compute(
        Vertex<CfLongId, FloatMatrixWritable, FloatWritable> arg0,
        Iterable<FloatMatrixMessage> arg1) throws IOException {
      // TODO Auto-generated method stub
      
    }
  }
  
  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute extends DefaultMasterCompute {
    private int maxIterations;
    private float rmseTarget;

    @Override
    public final void initialize() throws InstantiationException,
      IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
      registerPersistentAggregator(OVERALL_RATING_AGGREGATOR,
        DoubleSumAggregator.class);
      maxIterations = getContext().getConfiguration().getInt(ITERATIONS,
          ITERATIONS_DEFAULT);
      rmseTarget = getContext().getConfiguration().getFloat(RMSE_TARGET,
          RMSE_TARGET_DEFAULT);
    }

    @Override
    public final void compute() {
      long superstep = getSuperstep();
      if (superstep == 0) {
        setComputation(Svdpp.InitUsersComputation.class);
      } else if (superstep == 1) {
        setComputation(Svdpp.InitItemsComputation.class);
      } else if (superstep%2==0){
        setComputation(Svdpp.UserComputation.class);
      } else {
        setComputation(Svdpp.ItemComputation.class);
      }

      long numRatings = 0;
      double rmse = 0;

      if (superstep <= 2) {
        numRatings = getTotalNumEdges();
      } else {
        numRatings = getTotalNumEdges() / 2;
      }
      
      if (rmseTarget>0f) {
        rmse = Math.sqrt(((DoubleWritable)getAggregatedValue(RMSE_AGGREGATOR))
            .get() / numRatings);
      }

      if (rmseTarget>0f && rmse<rmseTarget) {
        haltComputation();
      } else if (superstep>maxIterations) {
        haltComputation();
      }
    }
  }
}
