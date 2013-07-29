package es.tid.graphlib.cf.als;

import java.util.Map.Entry;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.jblas.DoubleMatrix;
import org.jblas.Solve;

import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.TextMessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Alternating Least Squares (ALS)",
  description = "Matrix Factorization Algorithm: "
    + "It Minimizes the error in users preferences predictions")

public class Als extends Vertex<Text, AlsVertexValue,
  DoubleWritable, TextMessageWrapper> {
  /** Keyword for enabling RMSE aggregator. */
  public static final String RMSE_AGGREGATOR = "als.rmse.aggregator";
  /** Default value of RMSE aggregator tolerance. */
  public static final float RMSE_AGGREGATOR_DEFAULT = 0f;
  /** Keyword for specifying the halt factor. */
  public static final String HALT_FACTOR = "als.halt.factor";
  /** Default factor for halting execution. */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Keyword for parameter setting the convergence tolerance parameter.
   *  depending on the version enabled; l2norm or rmse */
  public static final String TOLERANCE_KEYWORD = "als.halting.tolerance";
  /** Default value for TOLERANCE. */
  public static final float TOLERANCE_DEFAULT = 1f;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS_KEYWORD = "als.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the regularization parameter LAMBDA. */
  public static final String LAMBDA_KEYWORD = "als.lambda";
  /** Default value for LABDA. */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the Latent Vector Size. */
  public static final String VECTOR_SIZE_KEYWORD = "als.vector.size";
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
  /** Factor Error: it may be RMSE or L2NORM on initial&final vector. */
  private double haltFactor = 0d;
  /** Number of updates. */
  private int updatesNum = 0;
  /** Type of vertex: true for item, false for item. */
  private boolean isItem = false;
  /** Initial vector value to be used for the L2Norm case.
   * Keep it outside the compute() method
   * value has to preserved throughout the supersteps
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
  public final void compute(final Iterable<TextMessageWrapper> messages) {
    /* Error = observed - predicted */
    double err = 0d;
    /* Flag for checking if parameter for RMSE aggregator received */
    float rmseTolerance = getContext().getConfiguration().getFloat(
      RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
    /*
     * Flag for checking which termination factor to use:
     * basic, rmse, l2norm
     **/
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
    /* Set the size of the Latent Vector*/
    int vectorSize = getContext().getConfiguration().getInt(
      VECTOR_SIZE_KEYWORD, VECTOR_SIZE_DEFAULT);

    // First superstep for users (superstep 0) & items (superstep 1)
    // Initialize vertex latent vector
    if (getSuperstep() < 2) {
      initLatentVector(vectorSize);
      // For L2Norm
      initialValue = new DoubleArrayListWritable(getValue().getLatentVector());
    }
    // Set flag for items - used in the Output Format
    if (getSuperstep() == 1) {
      isItem = true;
    }

    // Used if RMSE version or RMSE aggregator is enabled
    double rmseErr = 0d;

    // FOR LOOP - for each message
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
        addEdge(edge);
        // Remove the last value from message
        // It's there only for the 1st round of items
        message.getMessage().remove(message.getMessage().size() - 1);
      } // END OF IF CLAUSE - superstep==1

      // For the 1st superstep of either users or items, initialize their values
      // For the rest supersteps:
      // update their values based on the message received
      DoubleArrayListWritable currVal =
        getValue().getNeighValue(message.getSourceId());
      DoubleArrayListWritable newVal = message.getMessage();
      if (currVal == null || currVal.compareTo(newVal) != 0) {
        getValue().setNeighborValue(message.getSourceId(), newVal);
      }
    } // END OF LOOP - for each message

    if (getSuperstep() > 0) {
      // Execute ALS computation
      updateValue(lambda);
      // Used if RMSE version or RMSE aggregator is enabled
      rmseErr = 0d;
      // FOR LOOP - for each edge
      for (Entry<Text, DoubleArrayListWritable> vvertex
        :
        getValue().getAllNeighValue().entrySet()) {
        double observed = (double) getEdgeValue(vvertex.getKey()).get();
        err = getError(getValue().getLatentVector(), vvertex.getValue(),
          observed);
        // If termination flag is set to RMSE or RMSE aggregator is true
        if (factorFlag.equals("rmse") || rmseTolerance != 0f) {
          rmseErr += Math.pow(err, 2);
        }
      }
   } // END OF IF CLAUSE - Superstep > 0

    haltFactor =
      defineFactor(factorFlag, initialValue, tolerance, rmseErr);

    // If RMSE aggregator flag is true
    if (rmseTolerance != 0f) {
      this.aggregate(RMSE_AGGREGATOR, new DoubleWritable(rmseErr));
    }

    if (getSuperstep() == 0
      ||
        (haltFactor > tolerance && getSuperstep() < iterations)) {
      sendMessage();
    }
    // halt_factor is used in the OutputFormat file. --> To print the error
    if (factorFlag.equals("basic")) {
      haltFactor = err;
    }
    voteToHalt();
  } // END OF compute()

  /**
   * Initialize Vertex Latent Vector.
   *
   * @param vectorSize Vector Size
   */
  public final void initLatentVector(final int vectorSize) {
    AlsVertexValue value =
      new AlsVertexValue();
    for (int i = 0; i < vectorSize; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((Double.parseDouble(getId().toString().substring(2)) + i) % HUNDRED)
        / HUNDRED));
    }
    setValue(value);
  }

  /**
   * Modify Vertex Latent Vector based on ALS equation.
   *
   * @param lambda regularization parameter
   */
  public final void updateValue(final float lambda) {
    /**
     * Update Vertex Latent Vector based on ALS equation
     * Amat = MiIi * t(MiIi) + LAMBDA * Nui * E
     * Vmat = MiIi * t(R(i,Ii))
     * Amat * Umat = Vmat <==> solve Umat
     *
     * where MiIi: movies feature matrix rated by user i (matNeighVectors)
     * t(MiIi): transpose of MiIi (matNeighVectorsTrans)
     * Nui: number of ratings of user i (getNumEdges())
     * E: identity matrix (matId) R(i,Ii): ratings of movies rated by user i
     */
    int j = 0;
    DoubleMatrix matNeighVectors = new DoubleMatrix(getNumEdges());
    DoubleMatrix ratings = new DoubleMatrix(getNumEdges());
    // FOR LOOP - for each edge
    for (Entry<Text, DoubleArrayListWritable> vvertex
      : getValue().getAllNeighValue().entrySet()) {
      // Store the latent vector of the current neighbor
      double[] curVec = new double[vvertex.getValue().size()];
      for (int i = 0; i < vvertex.getValue().size(); i++) {
        curVec[i] = vvertex.getValue().get(i).get();
      }
      matNeighVectors.putColumn(j, new DoubleMatrix(curVec));
      // Store the rating related with the current neighbor
      ratings.put(j, (double) getEdgeValue(vvertex.getKey()).get());
      j++;
    } /// END OF LOOP - for each edge
    // Amat = MiIi * t(MiIi) + LAMBDA * getNumEdges() * matId
    DoubleMatrix matNeighVectorsTrans = matNeighVectors.transpose();
    DoubleMatrix matMul = matNeighVectors.mmul(matNeighVectorsTrans);
    DoubleMatrix matId = DoubleMatrix.eye(getValue().getNeighSize());
    double reg = lambda * getNumEdges();
    // Vmat = MiIi * t(R(i,Ii))
    DoubleMatrix aMatrix = matMul.add(matId.mul(reg));
    DoubleMatrix vMatrix = matNeighVectors.mmul(ratings);
    DoubleMatrix uMatrix = new DoubleMatrix();
    uMatrix = Solve.solve(aMatrix, vMatrix);

    // Update current vertex latent vector
    updateLatentVector(uMatrix);
    updatesNum++;
  }

  /**
   * Return the halt factor.
   *
   * @return haltFactor
   */
  final double returnHaltFactor() {
    return haltFactor;
  }

  /**
   * Update current vertex latent vector.
   *
   * @param value Vertex latent vector
   */
  public final void updateLatentVector(final DoubleMatrix value) {
    DoubleArrayListWritable val = new DoubleArrayListWritable();
    for (int i = 0; i < getValue().getLatentVector().size(); i++) {
      val.add(new DoubleWritable(value.get(i)));
      getValue().getLatentVector().set(i, new DoubleWritable(value.get(i)));
    }
    keepXdecimals(val, DECIMALS);
    //getValue().setLatentVector(val);
  }

  /**
   * Send message to neighbors.
   */
  public final void sendMessage() {
    // Send to all neighbors a message
    for (Edge<Text, DoubleWritable> edge : getEdges()) {
      // Create a message and wrap together the source id and the message
      TextMessageWrapper message = new TextMessageWrapper();
      message.setSourceId(getId());
      message.setMessage(getValue().getLatentVector());
      // At superstep 0, users send rating to items
      if (getSuperstep() == 0) {
        DoubleArrayListWritable x = new DoubleArrayListWritable(getValue()
          .getLatentVector());
        x.add(new DoubleWritable(edge.getValue().get()));
        message.setMessage(x);
      }
      sendMessage(edge.getTargetVertexId(), message);
    } // End of for each edge
  }

  /**
   * Decimal Precision of latent vector values.
   *
   * @param value Value to keep X decimals
   * @param x Amount of decimals
   */
  public final void keepXdecimals(final DoubleArrayListWritable value,
    final int x) {
    for (int i = 0; i < value.size(); i++) {
      value.set(i,
        new DoubleWritable(
          (double) (Math.round(
            value.get(i).get() * Math.pow(TEN, x - 1))
            /
            Math.pow(TEN, x - 1))));
    }
  }

  /**
   * Create a message and wrap together the source id and the message.
   * (and rating if applicable)
   *
   * @param id Vertex Id
   * @param vector Vertex latent vector
   * @param rating Rating of item
   *
   * @return TextMessageWrapper object
   */
  public final TextMessageWrapper wrapMessage(final Text id,
    final DoubleArrayListWritable vector, final int rating) {
    if (rating != -1) {
      vector.add(new DoubleWritable(rating));
    }
    return new TextMessageWrapper(id, vector);
  }

  /**
   * Calculate the RMSE on the errors calculated by the current vertex.
   *
   * @param rmseErr RMSE Error
   * @return RMSE result
   */
  public final double getRMSE(final double rmseErr) {
    return Math.sqrt(rmseErr / (double) messagesNum);
  }

  /**
   * Calculate the L2Norm on the initial and final value of vertex.
   *
   * @param valOld Old vertex vector
   * @param valNew New vertex vector
   *
   * @return sqrt(sum of all errors)
   */
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
   * where predicted = dotProduct (ma, mb)
   *
   * @param ma Matrix A
   * @param mb Matrix B
   * @param observed Observed value
   *
   * @return predicted - observed
   */
  public final double getError(final DoubleArrayListWritable ma,
    final DoubleArrayListWritable mb, final double observed) {
    // Convert ma,mb to DoubleMatrix
    // in order to use the dot-product method from jblas library
    DoubleMatrix matMa =
      convertDoubleArrayListWritable2DoubleMatrix(ma);
    DoubleMatrix matMb =
      convertDoubleArrayListWritable2DoubleMatrix(mb);
    // Predicted value
    double predicted = matMa.dot(matMb);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);

    return predicted - observed;
  }

  /**
   * Convert a DoubleMatrix (from jblas library) to DenseMatrix (from Mahout
   * library)
   * @param matrix matrix to be converted
   * @param xDimension Dimension x of matrix
   * @param yDimension Dimension y of matrix
   *
   * @return DenseMatrix
   ***/
  /*
  public DenseMatrix convertDoubleMatrix2Matrix(DoubleMatrix matrix,
      int xDimension, int yDimension) {
    double[][] amatDouble = new double[xDimension][yDimension];
    for (int i = 0; i < yDimension; i++) {
      for (int j = 0; j < yDimension; j++) {
        amatDouble[i][j] = matrix.get(i, j);
      }
    }
    return new DenseMatrix(amatDouble);
  }
*/
  /**
   * Convert a DoubleArrayListWritable (from graphlib library)
   * to DoubleMatrix (from jblas library).
   *
   * @param matrix The matrix to be converted
   *
   * @return convertedMatrix
   */
  public final DoubleMatrix convertDoubleArrayListWritable2DoubleMatrix(
      final DoubleArrayListWritable matrix) {
    DoubleMatrix convertedMatrix = new DoubleMatrix(matrix.size());

    for (int i = 0; i < matrix.size(); i++) {
      convertedMatrix.put(i, matrix.get(i).get());
    }
    return convertedMatrix;
  }

  /**
   * Return type of current vertex.
   *
   * @return boolean value if the current vertex behaves as an item
   */
  public final boolean isItem() {
    return isItem;
  }

  /**
   * Return amount of vertex updates.
   *
   * @return updatesNum
   */
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
   * Define whether the halt factor is "basic", "rmse" or "l2norm".
   *
   * @param factorFlag  Halt factor
   * @param pInitialValue Vertex initial value
   * @param pTolerance Tolerance
   * @param rmseErr RMSE error
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
        +
        " is not included in the recognized options");
    }
    return factor;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute extends DefaultMasterCompute {
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
    } // END OF compute()

    @Override
    public final void initialize() throws InstantiationException,
      IllegalAccessException {
      registerAggregator(RMSE_AGGREGATOR, DoubleSumAggregator.class);
    }
  }
}
