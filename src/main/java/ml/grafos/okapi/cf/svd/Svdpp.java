package ml.grafos.okapi.cf.svd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;
import ml.grafos.okapi.utils.Counters;

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
public class Svdpp {
  /** Name of aggregator that aggregates all ratings. */
  public static final String OVERALL_RATING_AGGREGATOR =
    "svd.overall.rating.aggregator";
  /** RMSE target */
  public static final String RMSE_TARGET = "svd.rmse.target";
  /** Default value for parameter enabling the RMSE aggregator. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
  /** Maximum number of iterations. */
  public static final String ITERATIONS = "svd.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Factor regularization parameter. */
  public static final String FACTOR_LAMBDA = "svd.lambda.factor";
  /** Default value for factor regularization parameter */
  public static final float FACTOR_LAMBDA_DEFAULT = 0.01f;
  /** Factor learning rate parameter */
  public static final String FACTOR_GAMMA = "svd.gamma.factor";
  /** Default value for factor learning rate */
  public static final float FACTOR_GAMMA_DEFAULT = 0.005f;
   /** Bias regularization parameter. */
  public static final String BIAS_LAMBDA = "svd.lambda.bias";
  /** Default value for bias regularization parameter */
  public static final float BIAS_LAMBDA_DEFAULT = 0.01f;
  /** Bias learning rate parameter */
  public static final String BIAS_GAMMA = "svd.gamma.bias";
  /** Default value for bias learning rate */
  public static final float BIAS_GAMMA_DEFAULT = 0.005f;
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
  
  private static final String COUNTER_GROUP = "SVD++ Counters";
  private static final String RMSE_COUNTER = "RMSE (x1000)";
  private static final String NUM_RATINGS_COUNTER = "# ratings";
  

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
   * Computes the updated baseline based on the formula:
   * 
   * b := b + gamma * (error - lambda * b)
   * 
   * @param baseline
   * @param predictedRating
   * @param observedRating
   * @param gamma
   * @param lambda
   */
  protected static final float computeUpdatedBaseLine(final float baseline, 
      final float predictedRating, final float observedRating, 
      final float gamma, final float lambda) {
    
    return baseline + 
        gamma*((predictedRating-observedRating)-lambda*baseline);
  }
  
  /**
   * Increments a scalar value according to the formula:
   * 
   * v:= v + step - gamma*lambda*v;
   * 
   * @param baseline
   * @param step
   * @param gamma
   * @param lambda
   * @return
   */
  protected static final float incrementValue(final float baseline, 
      final float step, final float gamma, final float lambda) {
    return baseline+step-gamma*lambda*baseline;
  }

  /**
   * Increments a vector according to the formula
   * 
   * v:= v + step - gamma*lambda*v
   * 
   * @param value
   * @param step
   * @param gamma
   * @param lambda
   */
  protected static void incrementValue(FloatMatrix value, FloatMatrix step, 
      final float gamma, final float lambda) {
    value.addi(value.mul(-gamma*lambda).addi(step));
  }

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
          new FloatMatrixWritable(0))); // The weights vector is empty for users
      
      // Send ratings to all items so that they can create the reverse edges.
      for (Edge<CfLongId, FloatWritable> edge : vertex.getEdges()) {
        FloatMatrixMessage msg = new FloatMatrixMessage(vertex.getId(), 
            new FloatMatrixWritable(0), // the matrix of this message is empty
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

    private float biasLambda;
    private float biasGamma;
    private float factorLambda;
    private float factorGamma;
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
      factorLambda = getContext().getConfiguration().getFloat(FACTOR_LAMBDA, 
          FACTOR_LAMBDA_DEFAULT);
      factorGamma = getContext().getConfiguration().getFloat(FACTOR_GAMMA, 
          FACTOR_GAMMA_DEFAULT);
      biasLambda = getContext().getConfiguration().getFloat(BIAS_LAMBDA, 
          BIAS_LAMBDA_DEFAULT);
      biasGamma = getContext().getConfiguration().getFloat(BIAS_GAMMA, 
          BIAS_GAMMA_DEFAULT);
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
      
      double rmsePartialSum = 0d;
      
      float userBaseline = vertex.getValue().getBaseline();
      int numRatings = vertex.getNumEdges();
      FloatMatrixWritable userFactors = vertex.getValue().getFactors();
      
      FloatMatrix sumWeights = new FloatMatrix(1,vectorSize);
      for (FloatMatrixMessage msg : messages) {
        // The weights are in the 2nd row of the matrix
        sumWeights.addi(msg.getFactors().getRow(1));
      }
      
      FloatMatrix itemWeightStep = new FloatMatrix(1,vectorSize);

      for (FloatMatrixMessage msg : messages) {
         // row 1 of the matrix in the message holds the item factors
        FloatMatrix itemFactors = msg.getFactors().getRow(0);
        // score holds the item baseline estimate
        float itemBaseline = msg.getScore();

        float observed = vertex.getEdgeValue(msg.getSenderId()).get();
        float predicted = computePredictedRating(
            meanRating, userBaseline, itemBaseline,
            userFactors, itemFactors,
            numRatings, sumWeights, minRating, maxRating);
        float error = predicted - observed;
        
        // Update baseline
        userBaseline = computeUpdatedBaseLine(userBaseline, predicted, 
            observed, biasGamma, biasLambda);
        
        // Update the value
        updateValue(userFactors, itemFactors, error, factorGamma, factorLambda);
        
        itemWeightStep.addi(itemFactors.mul(error));
      }
      
      vertex.getValue().setBaseline(userBaseline);

      itemWeightStep.muli(factorGamma/(float)Math.sqrt(numRatings));

      // Now we iterate again to get the new predictions and send the updates
      // to each item.
      for (FloatMatrixMessage msg : messages) {
        FloatMatrix itemFactors = msg.getFactors().getRow(0);
        float itemBaseline = msg.getScore();
        float observed = vertex.getEdgeValue(msg.getSenderId()).get();
        float predicted = computePredictedRating(
            meanRating, userBaseline, itemBaseline,
            userFactors, itemFactors,
            numRatings, sumWeights, minRating, maxRating);
        float error = predicted - observed;
        float itemBiasStep = biasGamma*error;
        FloatMatrix itemFactorStep = 
            sumWeights.mul(1f/(float)Math.sqrt(numRatings)).add(
                userFactors).mul(factorGamma*error);
        
        FloatMatrixWritable packedVectors = 
            new FloatMatrixWritable(2, vectorSize);
        packedVectors.putRow(0, itemFactorStep);
        packedVectors.putRow(1, itemWeightStep); 
        
        rmsePartialSum += (error*error);

        sendMessage(msg.getSenderId(), 
            new FloatMatrixMessage(
                vertex.getId(), packedVectors, itemBiasStep));
      }

      aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmsePartialSum));

      vertex.voteToHalt();
    }
  }
  
  public static class ItemComputation extends BasicComputation<CfLongId, 
  SvdppValue, FloatWritable, FloatMatrixMessage> {
    
    private float biasLambda;
    private float biasGamma;
    private float factorLambda;
    private float factorGamma;
    private int vectorSize;

    @Override
    public void preSuperstep() {
      biasLambda = getContext().getConfiguration().getFloat(BIAS_LAMBDA, 
          BIAS_LAMBDA_DEFAULT);
      biasGamma = getContext().getConfiguration().getFloat(BIAS_GAMMA, 
          BIAS_GAMMA_DEFAULT);  
      factorLambda = getContext().getConfiguration().getFloat(FACTOR_LAMBDA, 
          FACTOR_LAMBDA_DEFAULT);
      factorGamma = getContext().getConfiguration().getFloat(FACTOR_GAMMA, 
          FACTOR_GAMMA_DEFAULT);
      vectorSize = getContext().getConfiguration().getInt(VECTOR_SIZE, 
          VECTOR_SIZE_DEFAULT);
    }
    
    @Override
    public void compute(
        Vertex<CfLongId, SvdppValue, FloatWritable> vertex,
        Iterable<FloatMatrixMessage> messages) throws IOException {
      
      float itemBaseline = vertex.getValue().getBaseline();
      FloatMatrix itemFactors = vertex.getValue().getFactors();
      FloatMatrix itemWeights = vertex.getValue().getWeight();
      
      for (FloatMatrixMessage msg : messages) {
        float itemBiasStep = msg.getScore();
        FloatMatrix itemFactorStep = msg.getFactors().getRow(0);
        FloatMatrix itemWeightStep = msg.getFactors().getRow(1);
        
        itemBaseline = incrementValue(itemBaseline, itemBiasStep, biasGamma, 
            biasLambda);
        incrementValue(itemFactors, itemFactorStep, factorGamma, factorLambda);
        incrementValue(itemWeights, itemWeightStep, factorGamma, factorLambda);
      }
      
      FloatMatrixWritable packedVectors = 
          new FloatMatrixWritable(2, vectorSize);
      packedVectors.putRow(0, itemFactors);
      packedVectors.putRow(1, itemWeights); 

      sendMessageToAllEdges(vertex, 
          new FloatMatrixMessage(vertex.getId(), packedVectors, itemBaseline));
      
      vertex.getValue().setBaseline(itemBaseline);
      vertex.voteToHalt();
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
      
      rmse = Math.sqrt(((DoubleWritable)getAggregatedValue(RMSE_AGGREGATOR))
          .get() / numRatings);

      // Update the Hadoop counters
      Counters.updateCounter(getContext(), 
          COUNTER_GROUP, RMSE_COUNTER, 1000*(long)rmse);
      Counters.updateCounter(getContext(), 
          COUNTER_GROUP, NUM_RATINGS_COUNTER, numRatings);

      if (rmseTarget>0f && rmse<rmseTarget) {
        haltComputation();
      } else if (superstep>maxIterations) {
        haltComputation();
      }
    }
  }
}
