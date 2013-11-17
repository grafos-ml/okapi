package ml.grafos.okapi.cf.sgd;

import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.jblas.FloatMatrix;

/**
 * Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)", 
    description = "Minimizes the error in users preferences predictions")
public class Sgd extends BasicComputation<CfLongId, FloatMatrixWritable, 
  FloatWritable, FloatMatrixMessage> {
  
  /** Keyword for RMSE aggregator tolerance. */
  public static final String RMSE_TARGET = "sgd.rmse.target";
  /** Default value for parameter enabling the RMSE aggregator. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
  /** Keyword for parameter setting the convergence tolerance */
  public static final String TOLERANCE = "sgd.tolerance";
  /** Default value for TOLERANCE. */
  public static final float TOLERANCE_DEFAULT = -1f;
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
  public static final int VECTOR_SIZE_DEFAULT = 50;
  /** Max rating. */
  public static final String MAX_RATING = "sgd.max.rating";
  /** Default maximum rating */
  public static final float MAX_RATING_DEFAULT = 5.0f;
  /** Min rating. */
  public static final String MIN_RATING = "sgd.min.rating";
  /** Default minimum rating */
  public static final float MIN_RATING_DEFAULT = 0.0f;

  /** Aggregator used to compute the RMSE */
  public static final String RMSE_AGGREGATOR = "sgd.rmse.aggregator";

  private float tolerance;
  private float lambda;
  private float gamma;
  protected float minRating;
  protected float maxRating;
  private FloatMatrixWritable oldValue;

  @Override
  public void preSuperstep() {
    lambda = getContext().getConfiguration().getFloat(LAMBDA, LAMBDA_DEFAULT);
    gamma = getContext().getConfiguration().getFloat(GAMMA, GAMMA_DEFAULT);
    tolerance = getContext().getConfiguration().getFloat(TOLERANCE,
        TOLERANCE_DEFAULT);
    minRating = getContext().getConfiguration().getFloat(MIN_RATING, 
        MIN_RATING_DEFAULT);
    maxRating = getContext().getConfiguration().getFloat(MAX_RATING, 
        MAX_RATING_DEFAULT);
  }

  /**
   * Main SGD compute method.
   * 
   * @param messages
   *          Messages received
   */
  public final void compute(
      Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex,
      final Iterable<FloatMatrixMessage> messages) {
    
    double rmsePartialSum = 0d;
    float l2norm = 0f;

    if (tolerance>0) {
      // Create new object because we're going to operate on the old one.
      oldValue = new FloatMatrixWritable(vertex.getValue().getRows(), 
          vertex.getValue().getColumns(), vertex.getValue().data);
    }

    for (FloatMatrixMessage msg : messages) {
      // Get rating for the item that this message came from
      float rating = vertex.getEdgeValue(msg.getSenderId()).get();
      
      // Update the factors
      updateValue(vertex.getValue(), msg.getFactors(), rating, 
          minRating, maxRating, lambda, gamma);
      
      // Calculate new error
      float predicted = vertex.getValue().dot(msg.getFactors());
      predicted = Math.min(predicted, maxRating);
      predicted = Math.max(predicted, minRating);
      float err = predicted - rating;

      rmsePartialSum += Math.pow(err, 2);
    }

    aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmsePartialSum));

    // Calculate difference with previous value
    if (tolerance>0) {
      l2norm = vertex.getValue().distance2(oldValue);
    }
    
    // Broadcast the new vector
    if (tolerance<0 || (tolerance>0 && l2norm>tolerance)) {
      sendMessageToAllEdges(vertex, 
          new FloatMatrixMessage(vertex.getId(), vertex.getValue(), 0.0f));
    }
    
    vertex.voteToHalt();
  }

  /**
   * Applies the SGD update logic in the provided vector. It does the update
   * in-place.
   * 
   * The update is peformed according to the following formula:
   * 
   * v = v - gamma*(lambda*v + error*u)
   * 
   * @param value The vector to update
   * @param update The vector used to update
   * @param lambda
   * @param gamma
   * @param err
   */
  protected final void updateValue(FloatMatrix value, 
      FloatMatrix update, final float rating, final float minRatings, 
      final float maxRating, final float lambda, final float gamma) {
    
    float predicted = value.dot(update);
    
    // Correct the predicted rating
    predicted = Math.min(predicted, maxRating);
    predicted = Math.max(predicted, minRating);
    
    float err = predicted - rating;
    
    FloatMatrix part1 = value.mul(lambda);
    FloatMatrix part2 = update.mul(err);
    FloatMatrix part3 = (part1.add(part2)).mul(-gamma);
    value.addi(part3);
  }
  

  /**
   * This computation class is used to initialize the factors of the user nodes
   * in the very first superstep, and send the first updates to the item nodes.
   * @author dl
   *
   */
  public static class InitUsersComputation extends BasicComputation<CfLongId, 
  FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {

    @Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex,
        Iterable<FloatMatrixMessage> messages) throws IOException {
      
      FloatMatrixWritable vector = 
          new FloatMatrixWritable(getContext().getConfiguration().getInt(
          VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
      Random randGen = new Random();
      for (int i=0; i<vector.length; i++) {
        vector.put(i, 0.01f*randGen.nextFloat());
      }
      vertex.setValue(vector);
      
      for (Edge<CfLongId, FloatWritable> edge : vertex.getEdges()) {
        FloatMatrixMessage msg = new FloatMatrixMessage(
            vertex.getId(), vertex.getValue(), edge.getValue().get());
        sendMessage(edge.getTargetVertexId(), msg);
      }
      vertex.voteToHalt();
    }
  }
  
  /**
   * This computation class is used to initialize the factors of the item nodes
   * in the second superstep. Every item also creates the edges that point to
   * the users that have rated the item. 
   * @author dl
   *
   */
  public static class InitItemsComputation extends AbstractComputation<CfLongId, 
  FloatMatrixWritable, FloatWritable, FloatMatrixMessage,
  FloatMatrixMessage> {

    @Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, 
        FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) 
            throws IOException {
      
      FloatMatrixWritable vector = 
          new FloatMatrixWritable(getContext().getConfiguration().getInt(
          VECTOR_SIZE, VECTOR_SIZE_DEFAULT));
      Random randGen = new Random();
      for (int i=0; i<vector.length; i++) {
        vector.put(i, 0.01f*randGen.nextFloat());
      }
      vertex.setValue(vector);
      
      for (FloatMatrixMessage msg : messages) {
        DefaultEdge<CfLongId, FloatWritable> edge = 
            new DefaultEdge<CfLongId, FloatWritable>();
        edge.setTargetVertexId(msg.getSenderId());
        edge.setValue(new FloatWritable(msg.getScore()));
        vertex.addEdge(edge);
      }
      
      // The score does not matter at this point.
      sendMessageToAllEdges(vertex, 
          new FloatMatrixMessage(vertex.getId(), vertex.getValue(), 0.0f));
      
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
      maxIterations = getContext().getConfiguration().getInt(ITERATIONS,
          ITERATIONS_DEFAULT);
      rmseTarget = getContext().getConfiguration().getFloat(RMSE_TARGET,
          RMSE_TARGET_DEFAULT);
    }

    @Override
    public final void compute() {
      long superstep = getSuperstep();
      if (superstep == 0) {
        setComputation(Sgd.InitUsersComputation.class);
      } else if (superstep == 1) {
        setComputation(Sgd.InitItemsComputation.class);
      } else {
        setComputation(Sgd.class);
      }
      
      double numRatings = 0;
      double rmse = 0;

      // Until superstep 2 only half edges are created (users to items)
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
