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
package ml.grafos.okapi.cf.als;

import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.Parameters;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;
import ml.grafos.okapi.examples.SimpleMasterComputeVertex;
import ml.grafos.okapi.utils.Counters;

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
import org.jblas.JavaBlas;
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
  public static final String RMSE_TARGET = "rmse";
  /** Default value of RMSE target. */
  public static final float RMSE_TARGET_DEFAULT = -1f;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS = "iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the regularization parameter LAMBDA. */
  public static final String LAMBDA = "lambda";
  /** Default value for LABDA. */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the Latent Vector Size. */
  public static final String VECTOR_SIZE = "dim";
  /** Default value for vector size. */
  public static final int VECTOR_SIZE_DEFAULT = 50;
  
  /** Aggregator used to compute the RMSE */
  public static final String RMSE_AGGREGATOR = "als.rmse.aggregator";

  private static final String COUNTER_GROUP = "ALS Counters";
  private static final String RMSE_COUNTER = "RMSE (x1000)";
  private static final String NUM_RATINGS_COUNTER = "# ratings";
  private static final String RMSE_COUNTER_GROUP = "RMSE Counters";
  
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
      mat_R.put(i, 0, vertex.getEdgeValue(msg.getSenderId()).get());
      i++;
    } 
     
    updateValue(vertex.getValue(), mat_M, mat_R, lambda);
    
    // Calculate errors and add squares to the RMSE aggregator
    double rmsePartialSum = 0d;
    for (int j=0; j<mat_M.columns; j++) {    
        float prediction = vertex.getValue().dot(mat_M.getColumn(j));
        double error = prediction - mat_R.get(j, 0);
        rmsePartialSum += (error*error);
    }
    
    aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmsePartialSum));

    // Propagate new value
    sendMessageToAllEdges(vertex, 
        new FloatMatrixMessage(vertex.getId(), vertex.getValue(), 0.0f));
    
    vertex.voteToHalt();
  } 

  protected void updateValue(FloatMatrix value, FloatMatrix mat_M, 
      FloatMatrix mat_R, final float lambda) {
    
    FloatMatrix mat_V = mat_M.mmul(mat_R);
    FloatMatrix mat_A = mat_M.mmul(mat_M.transpose());
    mat_A.addi(FloatMatrix.eye(mat_M.rows).muli(lambda*mat_R.rows)); 
    
    FloatMatrix mat_U = Solve.solve(mat_A, mat_V);
    value.rows = mat_U.rows;
    value.columns = mat_U.columns;
    JavaBlas.rcopy(mat_U.length, mat_U.data, 0, 1, value.data, 0, 1);
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
      
      long numRatings = 0;
      double rmse = 0;

      // Until superstep 2 only half edges are created (users to items)
      if (getSuperstep() <= 2) {
        numRatings = getTotalNumEdges();
      } else {
        numRatings = getTotalNumEdges() / 2;
      }

      rmse = Math.sqrt(((DoubleWritable)getAggregatedValue(RMSE_AGGREGATOR))
          .get() / numRatings);
      
      if (Parameters.DEBUG.get(getContext().getConfiguration()) 
          && superstep>2) {
        Counters.updateCounter(getContext(), RMSE_COUNTER_GROUP, 
            "Iteration "+(getSuperstep()-2), (long)(1000*rmse));
      }
      
      // Update the Hadoop counters
      Counters.updateCounter(getContext(), 
          COUNTER_GROUP, RMSE_COUNTER, (long)(1000*rmse));
      Counters.updateCounter(getContext(), 
          COUNTER_GROUP, NUM_RATINGS_COUNTER, numRatings);

      if (rmseTarget>0f && rmse<rmseTarget) {
        haltComputation();
      } else if (getSuperstep()>maxIterations) {
        haltComputation();
      }
    }
  }
}
