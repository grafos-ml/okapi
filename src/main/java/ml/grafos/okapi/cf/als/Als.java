package ml.grafos.okapi.cf.als;

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
import org.jblas.Solve;


/**
 * Alternating Least Squares (ALS) implementation.
 */
@Algorithm(
  name = "Alternating Least Squares (ALS)",
  description = "Matrix Factorization Algorithm: "
    + "It Minimizes the error in users preferences predictions")

public class Als extends BasicComputation<CfLongId, FloatMatrixWritable,
  FloatWritable, FloatMatrixMessage> {
  
  /** RMSE target to reach. */
  public static final String RMSE_TARGET = "als.rmse.target";
  /** Default value of RMSE target. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
//  /** Keyword for parameter setting of the convergence tolerance */
//  public static final String TOLERANCE = "als.tolerance";
//  /** Default value for TOLERANCE. */
//  public static final float TOLERANCE_DEFAULT = -1f;
//  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS = "als.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the regularization parameter LAMBDA. */
  public static final String LAMBDA = "als.lambda";
  /** Default value for LABDA. */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the Latent Vector Size. */
  public static final String VECTOR_SIZE = "als.vector.size";
  /** Default value for GAMMA. */
  public static final int VECTOR_SIZE_DEFAULT = 50;
//  /** Max rating. */
//  public static final String MAX_RATING = "als.max.rating";
//  /** Max rating default. */
//  public static final double MAX_RATING_DEFAULT = 5;
//  /** Min rating. */
//  public static final double MIN_RATING = 0;
//  /** Min rating default */
//  public static final double MIN_RATING_DEFAULT = 0;
  
  /** Aggregator used to compute the RMSE */
  public static final String RMSE_AGGREGATOR = "als.rmse.aggregator";

  private float lambda;
  private int vectorSize;
  
  
  @Override
  public void preSuperstep() {
    lambda = getContext().getConfiguration().getFloat(LAMBDA, LAMBDA_DEFAULT);
    vectorSize = getContext().getConfiguration().getInt(VECTOR_SIZE, 
        VECTOR_SIZE_DEFAULT);
  }
  
  /**
   * Main ALS compute method.
   * 
   * It updates the current latent vector based on ALS:<br>
   *  A = M * M^T + LAMBDA * N * E<br>
   *  V = M * R<br>
   *  A * U = V, then solve for U<br>
   *  <br> 
   *  where<br>
   *  R: column vector with ratings by this user<br>
   *  M: item features for items rated by this user with dimensions |F|x|R|<br>
   *  M^T: transpose of M with dimensions |R|x|F|<br>
   *  N: number of ratings of this user<br>
   *  E: identity matrix with dimensions |F|x|F|<br>
   * 
   * @param messages Messages received
   */
  public final void compute(
      Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, 
      final Iterable<FloatMatrixMessage> messages) {
    
    FloatMatrix mat_M = new FloatMatrix(vectorSize, vertex.getNumEdges());
    FloatMatrix mat_R = new FloatMatrix(vertex.getNumEdges(), 1); 
    
    // Build the matrices of the linear system
    int i=0;
    for (FloatMatrixMessage msg : messages) {
      mat_M.putColumn(i, msg.getFactors());
      mat_R.put(0, i, vertex.getEdgeValue(msg.getSenderId()).get());
      i++;
    } 
     
    // We do all the matrix operations in-place to save memory
    
    mat_M.mmuli(mat_R, mat_R); // mat_R now contains the value of V
    mat_M.mmuli(mat_M.transpose(), mat_M); // mat_M now is M*M^T
    mat_M.addi(FloatMatrix.eye(vectorSize).muli(lambda*vertex.getNumEdges()));
    
    FloatMatrix mat_U = Solve.solve(mat_M, mat_R);
    
    // We need a copy that is Writable to set as the vertex value
    vertex.setValue(new FloatMatrixWritable(mat_U));
    
    // Calculate errors and add squares to the RMSE aggregator
    double rmsePartialSum = 0d;
    for (int j=0; j<mat_M.columns; j++) {    
        float prediction = mat_U.dot(mat_M.getColumn(j));
        double error = prediction - mat_R.get(j);
        rmsePartialSum += Math.pow(error, 2);
    }
    
    aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmsePartialSum));

    // Propagate new value
    sendMessageToAllEdges(vertex, 
        new FloatMatrixMessage(vertex.getId(), vertex.getValue(), 0.0f));
    
    vertex.voteToHalt();
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
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
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
        setComputation(Als.InitUsersComputation.class);
      } else if (superstep == 1) {
        setComputation(Als.InitItemsComputation.class);
      } else {
        setComputation(Als.class);
      }
      
      double numRatings = 0;
      double rmse = 0;

      if (getSuperstep() > 1) {
        // In superstep=1 only half edges are created (users to items)
        if (getSuperstep() == 2) {
          numRatings = getTotalNumEdges();
        } else {
          numRatings = getTotalNumEdges() / 2;
        }
      }

      if (rmseTarget>0f) {
        rmse = Math.sqrt(((DoubleWritable)getAggregatedValue(RMSE_AGGREGATOR))
            .get() / numRatings);
//        System.out.println("Superstep:"+getSuperstep() + ", RMSE: "+rmse);
      }

      if (rmseTarget>0f && rmse<rmseTarget) {
        haltComputation();
      } else if (getSuperstep()>maxIterations) {
        haltComputation();
      }
    }
  }
}
