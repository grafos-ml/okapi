package es.tid.graphlib.als;

import java.util.Map.Entry;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.Vector;
import org.jblas.DoubleMatrix;

import es.tid.graphlib.utils.DoubleArrayListHashMapWritable;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Alternating Least Squares (ALS)",
  description = "Matrix Factorization Algorithm: " +
      "It Minimizes the error in users preferences predictions")

public class Als extends Vertex<IntWritable, DoubleArrayListHashMapWritable,
  IntWritable, MessageWrapper> {
  /** Keyword for enabling RMSE aggregator */
  public static final String RMSE_AGGREGATOR = "als.rmse.aggregator";
  /** Default boolean value of RMSE aggregator */
  public static final boolean RMSE_AGGREGATOR_DEFAULT = false;
  /** Keyword for specifying the halt factor */
  public static final String HALT_FACTOR = "als.halt.factor";
  /** Default factor for halting execution */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Vector size **/
  public static final int VECTOR_SIZE = 2;
  /** Regularization parameter */
  public static final double LAMBDA = 0.005;
  /** Number of supersteps */
  public static final double ITERATIONS = 10;
  /** Convergence Tolerance */
  public static final double TOLERANCE = 0.0003;
  /** Max rating */
  public static final double MAX = 5;
  /** Min rating */
  public static final double MIN = 0;
  /** Decimals */
  public static final int DECIMALS = 4;
  /** Aggregator to get values from the workers to the master */
  public static final String RMSE_AGG = "rmse.aggregator";
  /** Factor Error: it may be RMSE or L2NORM on initial&final vector */
  private double haltFactor = 0d;
  /** Number of updates */
  private int updatesNum = 0;
  /** Observed Value - Rating */
  private double observed = 0d;
  /** RMSE Error */
  private double rmseErr = 0d;
  /** Type of vertex: true for item, false for item */
  private boolean item = false;

  /**
   * Compute method
   *
   * @param messages Messages received
   */
  public void compute(Iterable<MessageWrapper> messages) {
    /*
     * Counter of messages received - different from getNumEdges() because a
     * neighbor may not send a message
     */
    int msgCounter = 0;
    /* Error = observed - predicted */
    double err = 0d;
    /* Flag for checking if parameter for RMSE aggregator received */
    boolean rmseFlag = getContext().getConfiguration().getBoolean(
      RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
    /* Initial vector value to be used for the L2Norm case */
    DoubleArrayListWritable initialValue = new DoubleArrayListWritable();
    /*
     * Flag for checking which termination factor to use:
     * basic, rmse, l2norm
     **/
    String factorFlag = getContext().getConfiguration().get(HALT_FACTOR,
      HALT_FACTOR_DEFAULT);

    /* First superstep for users (superstep: 0) & items (superstep: 1) */
    if (getSuperstep() < 2) {
      initLatentVector(initialValue);
    }
    /* Set flag for items */
    if (getSuperstep() == 1) {
      item = true;
    }
    System.out.println("*******  Vertex: " + getId() + ", superstep:" +
      getSuperstep() + ", item:" + item +
        ", " + getValue().getLatentVector());

    /* For each message */
    for (MessageWrapper message : messages) {
      msgCounter++;
      System.out.println("  [RECEIVE] from " + message.getSourceId().get() +
        ", " + message.getMessage());

      /*
       * First superstep for items
       * 1. Add Edges connecting to users
       * 2. Store the rating given from users
       */
      if (getSuperstep() == 1) {
        observed = message.getMessage().get(message.getMessage().size() - 1)
          .get();
        DefaultEdge<IntWritable, IntWritable> edge =
          new DefaultEdge<IntWritable, IntWritable>();
        edge.setTargetVertexId(message.getSourceId());
        edge.setValue(new IntWritable((int) observed));
        addEdge(edge);
        // Remove the last value from message - it's there for the 1st round
        message.getMessage().remove(message.getMessage().size() - 1);
      }
      /* Create table with neighbors latent values and ids */
      if (getSuperstep() == 1 || getSuperstep() == 2) {
        getValue().setNeighborValue(message.getSourceId(),
          message.getMessage());
        // for (int i=0; i<getValue().getAllNeighValue().size(); i++)
        // getValue().getNeighValue(i).
      }
      if (getSuperstep() > 2) {
        updateNeighValues(getValue().getNeighValue(message.getSourceId()),
          message.getMessage());
      }
    } // Eof Messages

    if (getSuperstep() > 0) {
      for (Entry<IntWritable, DoubleArrayListWritable> vvertex : getValue()
        .getAllNeighValue().entrySet()) {
        /* Calculate error */
        observed = (double) getEdgeValue(vvertex.getKey()).get();
        err = getError(getValue().getLatentVector(), vvertex.getValue(),
          observed);
        System.out.print("BEFORE: error = " + err + " vertex_vector= " +
          getValue().getLatentVector() + " vv: " + vvertex.getKey() + ", ");
        vvertex.getValue().print();
      }
      runAlsAlgorithm();
      rmseErr = 0d;
      for (Entry<IntWritable, DoubleArrayListWritable> vvertex : getValue()
        .getAllNeighValue().entrySet()) {
        observed = (double) getEdgeValue(vvertex.getKey()).get();
        err = getError(getValue().getLatentVector(), vvertex.getValue(),
          observed);
        System.out.print("AFTER: error = " + err + " vertex_vector= " +
          getValue().getLatentVector() + " vv: " + vvertex.getKey());
        vvertex.getValue().print();
        /* If termination flag is set to RMSD or RMSD aggregator is true */
        if (factorFlag.equals("rmse") || rmseFlag) {
          rmseErr += Math.pow(err, 2);
        }
      }
    } // Eof Superstep>0

    haltFactor = defineFactor(factorFlag, msgCounter, initialValue);

    /* If RMSE aggregator flag is true */
    if (rmseFlag) {
      this.aggregate(RMSE_AGG, new DoubleWritable(rmseErr));
    }

    if (getSuperstep() == 0 ||
        (haltFactor > TOLERANCE && getSuperstep() < ITERATIONS)) {
      sendMessage();
    }
    /* halt_factor is used in the OutputFormat file. --> To print the error */
    if (factorFlag.equals("basic")) {
      haltFactor = err;
    }
    voteToHalt();
  } // Eof compute()

  /**
   * Initialize Vertex Latent Vector
   *
   * @param initialValue Vertex value - to get initialized here
   *
   * @return initialValue Vertex value initialized
   */
  public DoubleArrayListWritable
  initLatentVector(DoubleArrayListWritable initialValue) {
    DoubleArrayListHashMapWritable value =
      new DoubleArrayListHashMapWritable();
    for (int i = 0; i < VECTOR_SIZE; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((double) (getId().get() + i) % 100d) / 100d));
    }
    setValue(value);
    /* For L2Norm */
    initialValue = getValue().getLatentVector();
    return initialValue;
  }

  /***
   * Update Vertex Latent Vector based on ALS equation Amat = MiIi * t(MiIi) +
   * LAMBDA * Nui * E Vmat = MiIi * t(R(i,Ii)) Amat * Umat = Vmat <==> solve
   * Umat
   *
   * where MiIi: movies feature matrix rated by user i (matNeighVectors)
   * t(MiIi): transpose of MiIi (matNeighVectorsTrans) Nui: number of ratings
   * of user i (getNumEdges()) E: identity matrix (matId) R(i,Ii): ratings of
   * movies rated by user i
   */
  public void runAlsAlgorithm() {
    int j = 0;
    DoubleMatrix matNeighVectors =
      new DoubleMatrix(VECTOR_SIZE, getNumEdges());
    double[] curVec = new double[VECTOR_SIZE];
    DoubleMatrix ratings = new DoubleMatrix(getNumEdges());
    /* Go through the neighbors */
    for (Entry<IntWritable, DoubleArrayListWritable> vvertex : getValue()
      .getAllNeighValue().entrySet()) {
      /* Store the latent vector of the current neighbor */
      for (int i = 0; i < VECTOR_SIZE; i++) {
        curVec[i] = vvertex.getValue().get(i).get();
      }
      matNeighVectors.putColumn(j, new DoubleMatrix(curVec));

      /* Store the rating related with the current neighbor */
      ratings.put(j, (double) getEdgeValue(vvertex.getKey()).get());
      j++;
    } // Eof Neighbours Edges

    /* Amat = MiIi * t(MiIi) + LAMBDA * getNumEdges() * matId */
    // System.out.println("matNeigh * matNeighTrans = matMul");
    DoubleMatrix matNeighVectorsTrans = matNeighVectors.transpose();
    DoubleMatrix matMul = matNeighVectors.mmul(matNeighVectorsTrans);
    // matNeighVectors.print();
    // matNeighVectorsTrans.print();
    // matMul.print();

    DoubleMatrix matId = new DoubleMatrix();
    double reg = LAMBDA * getNumEdges();
    matId = matId.eye(VECTOR_SIZE);
    // System.out.println("matMul + (reg=" + reg + " * matId)");
    // matMul.print();
    // matId.print();
    DoubleMatrix aMatrix = matMul.add(matId.mul(reg));

    /* Vmat = MiIi * t(R(i,Ii)) */
    // System.out.println("matNeigh * transpose(ratings)");
    // matNeighVectors.print();
    // ratings.print();
    DoubleMatrix vMatrix = matNeighVectors.mmul(ratings);

    // System.out.println("aMatrix, vMatrix");
    // aMatrix.print();
    // vMatrix.print();

    /*
     * Amat * Umat = Vmat <==> solve Umat Convert Amat and Vmat into type:
     * DenseMatrix in order to use the QRDecomposition method from Mahout
     */
    DenseMatrix aDenseMatrix =
      convertDoubleMatrix2Matrix(aMatrix, VECTOR_SIZE, VECTOR_SIZE);
    DenseMatrix vDenseMatrix =
      convertDoubleMatrix2Matrix(vMatrix, VECTOR_SIZE, 1);
    Vector uMatrix =
      new QRDecomposition(aDenseMatrix).solve(vDenseMatrix).viewColumn(0);

    /* Update current vertex latent vector */
    updateLatentVector(uMatrix);
    // System.out.println("v: " + getValue().getLatentVector());
    incUpdatesNum(updatesNum);
  }

  /**
   * Return the halt factor
   * @return haltFactor
   */
  double returnHaltFactor() {
    return haltFactor;
  }

  /** Increase the number of updates
   *
   * @param updatesNum counter of updates
   */
  public void incUpdatesNum(int updatesNum) {
    updatesNum++;
  }
  /**
   * Update current vertex latent vector
   *
   * @param value Vertex latent vector
   */
  public void updateLatentVector(Vector value) {
    DoubleArrayListWritable val = new DoubleArrayListWritable();
    for (int i = 0; i < VECTOR_SIZE; i++) {
      val.add(new DoubleWritable(value.get(i)));
      keepXdecimals(val, DECIMALS);
      getValue().setLatentVector(val);
    }
  }

  /**
   * Update neighbor's values
   *
   * @param curVal Current vertex value
   * @param latestVal Latest vertex value
   *
   * @return updated a boolean value if updated is done successfully
   */
  public boolean updateNeighValues(DoubleArrayListWritable curVal,
    DoubleArrayListWritable latestVal) {
    boolean updated = false;
    for (int i = 0; i < VECTOR_SIZE; i++) {
      if (latestVal.get(i) != curVal.get(i)) {
        curVal.set(i, latestVal.get(i));
        updated = true;
      } /*else {
        System.out.println("[COMPARE]" + curVal.get(i) + ", " +
          latestVal.get(i));
      } */
    }
    return updated;
  }

  /**
   * Send message to neighbors
   */
  public void sendMessage() {
    /* Send to all neighbors a message */
    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
      /* Create a message and wrap together the source id and the message */
      MessageWrapper message = new MessageWrapper();
      message.setSourceId(getId());
      message.setMessage(getValue().getLatentVector());
      // 1st superstep, users send rating to items
      if (getSuperstep() == 0) {
        DoubleArrayListWritable x = new DoubleArrayListWritable(getValue()
          .getLatentVector());
        x.add(new DoubleWritable(edge.getValue().get()));
        message.setMessage(x);
      }
      sendMessage(edge.getTargetVertexId(), message);
      /*
       * System.out.println("  [SEND] to " + edge.getTargetVertexId() +
       * " (rating: " + edge.getValue() + ")" + " [" +
       * getValue().getLatentVector() + "]");
       */
    } // End of for each edge
  }

  /**
   * Decimal Precision of latent vector values
   *
   * @param value Value to keep X decimals
   * @param x Amount of decimals
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

  /**
   * Create a message and wrap together the source id and the message
   * (and rating if applicable)
   *
   * @param id Vertex Id
   * @param vector Vertex laten vector
   * @param rating Rating of item
   *
   * @return MessageWrapper object
   */
  public MessageWrapper wrapMessage(IntWritable id,
    DoubleArrayListWritable vector, int rating) {
    if (rating != -1) {
      vector.add(new DoubleWritable(rating));
    }
    return new MessageWrapper(id, vector);
  }

  /**
   * Calculate the RMSE on the errors calculated by the current vertex
   *
   * @param msgCounter Amount of messages received
   *
   * @return sqrt(rmse error / msgCounter)
   */
  public double getRMSE(int msgCounter) {
    return Math.sqrt(rmseErr / msgCounter);
  }

  /**
   * Calculate the RMSE on the errors calculated by the current vertex
   *
   * @param valOld Old vertex vector
   * @param valNew New vertex vector
   *
   * @return sqrt(sum of all errors)
   */
  public double getL2Norm(DoubleArrayListWritable valOld,
    DoubleArrayListWritable valNew) {
    double result = 0;
    for (int i = 0; i < valOld.size(); i++) {
      result += Math.pow(valOld.get(i).get() - valNew.get(i).get(), 2);
    }
    //System.out.println("L2norm: " + result);
    return Math.sqrt(result);
  }

  /**
   * Calculate the error: e = observed - predicted,
   * where predicted = dotProduct (ma, mb)
   *
   * @param ma Matrix A
   * @param mb Matrix B
   * @param observed Observed value
   *
   * @return predicted - observed
   */
  public double getError(DoubleArrayListWritable ma,
    DoubleArrayListWritable mb, double observed) {
    /*
     * Convert ma,mb to DoubleMatrix in order to use the dot-product method
     * from jblas library
     */
    DoubleMatrix matMa =
      convertDoubleArrayListWritable2DoubleMatrix(ma, ma.size());
    DoubleMatrix matMb =
      convertDoubleArrayListWritable2DoubleMatrix(mb, mb.size());
    /** Predicted value */
    double predicted = matMa.dot(matMb);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);

    return predicted - observed;
  }

  /***
   * Convert a DoubleMatrix (from jblas library) to DenseMatrix (from Mahout
   * library)
   * @param matrix matrix to be converted
   * @param xDimension Dimension x of matrix
   * @param yDimension Dimension y of matrix
   *
   * @return DenseMatrix
   ***/
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

  /**
   * Convert a DoubleArrayListWritable (from graphlib library) to DoubleMatrix
   * (from jblas library)
   * @param matrix The matrix to be converted
   * @param size Size of the matrix
   *
   * @return convertedMatrix
   */
  public DoubleMatrix convertDoubleArrayListWritable2DoubleMatrix(
      DoubleArrayListWritable matrix, int size) {
    DoubleMatrix convertedMatrix = new DoubleMatrix(size);

    for (int i = 0; i < size; i++) {
      convertedMatrix.put(i, matrix.get(i).get());
    }
    return convertedMatrix;
  }

  /**
   * Return type of current vertex
   *
   * @return boolean value if the current vertex behaves as an item
   */
  public boolean isItem() {
    return item;
  }

  /**
   * Return amount of vertex updates
   *
   * @param updatesNum Counter of updates
   *
   * @return updatesNum
   */
  public int getUpdates() {
    return updatesNum;
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
  public double defineFactor(String factorFlag, int msgCounter,
      DoubleArrayListWritable initialValue) {
    double factor = 0d;
    switch (factorFlag) {
    case "basic":
      factor = TOLERANCE + 1d;
      break;
    case "rmse":
      factor = getRMSE(msgCounter);
      break;
    case "l2norm":
      factor = getL2Norm(initialValue, getValue().getLatentVector());
      break;
    default:
      throw new RuntimeException("BUG: halt factor " + factorFlag +
        " is not included in the recognized options");
    }
    return factor;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute extends DefaultMasterCompute {
    @Override
    public void compute() {
      double numRatings = 0;
      double totalRMSE = 0;
      if (getSuperstep() > 1) {
        // In superstep=1 only half edges are created (users to items)
        if (getSuperstep() == 2) {
          numRatings = getTotalNumEdges();
        } else {
          numRatings = getTotalNumEdges() / 2;
        }
        totalRMSE = Math.sqrt(((DoubleWritable)
          getAggregatedValue(RMSE_AGG)).get() / numRatings);

        /* System.out.println("Superstep: " + getSuperstep() +
          ", [Aggregator] Added Values: " + getAggregatedValue(RMSE_AGG) +
            " / " + numRatings + " = " +
              ((DoubleWritable) getAggregatedValue(RMSE_AGG)).get() /
                numRatings + " --> sqrt(): " + totalRMSE); */

        getAggregatedValue(RMSE_AGG);
        if (totalRMSE < TOLERANCE) {
          //System.out.println("HALT!");
          haltComputation();
        }
      }
    } // Eof Compute()

    @Override
    public void initialize() throws InstantiationException,
      IllegalAccessException {
      registerAggregator(RMSE_AGG, DoubleSumAggregator.class);
    }
  }
}
