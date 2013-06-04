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
  IntWritable, MessageWrapper> {
  /** Keyword for parameter enabling delta caching */
  public static final String DELTA_CACHING = "sgd.delta.caching";
  /** Default value for parameter enabling delta caching */
  public static final boolean DELTA_CACHING_DEFAULT = false;
  /** Keyword for parameter enabling the RMSE aggregator */
  public static final String RMSE_AGGREGATOR = "sgd.rmse.aggregator";
  /** Default value for parameter enabling the RMSE aggregator */
  public static final boolean RMSE_AGGREGATOR_DEFAULT = false;
  /** Keyword for parameter choosing the halt factor */
  public static final String HALT_FACTOR = "sgd.halt.factor";
  /** Default value for parameter choosing the halt factor */
  public static final String HALT_FACTOR_DEFAULT = "basic";
  /** Keyword for parameter setting the number of iterations */
  public static final String PARAM_ITERATIONS = "sgd.iterations";
  /** Default value for parameter choosing the halt factor */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the convergence tolerance parameter
   *  depending on the version enabled; l2norm or rmse */
  public static final String PARAM_TOLERANCE = "sgd.tolerance";
  /** Default value for parameter choosing the halt factor */
  public static final int TOLERANCE_DEFAULT = 1;
  /** Vector Size **/
  public static final int VECTOR_SIZE = 2;
  /** Regularization parameter */
  public static final double LAMBDA = 0.005;
  /** Learning rate */
  public static final double GAMMA = 0.01;
  /** Max rating */
  public static final double MAX = 5;
  /** Min rating */
  public static final double MIN = 0;
  /** Decimals */
  public static final int DECIMALS = 4;
  /** Aggregator to get values from the workers to the master */
  public static final String RMSE_AGG = "rmse.aggregator";
  /** Factor Error: it may be RMSD or L2NORM on initial&final vector */
  private double haltFactor = 0d;
  /** Number of updates */
  private int nupdates = 0;
  /** Observed Value - Rating */
  private double observed = 0d;
  /** Error */
  private double err = 0d;
  /** RMSE Error */
  private double rmseErr = 0d;
  /** Initial vector value to be used for the L2Norm case */
  private DoubleArrayListWritable initialValue =
    new DoubleArrayListWritable();
  /** Type of vertex 0 for user, 1 for item */
  private boolean item = false;

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<MessageWrapper> messages) {
    /**
     * Counter of messages received - different from getNumEdges() because a
     * neighbor may not send a message
     */
    int msgCounter = 0;
    /** Flag for checking if parameter for RMSE aggregator received */
    boolean rmseFlag = getContext().getConfiguration().getBoolean(
      RMSE_AGGREGATOR, RMSE_AGGREGATOR_DEFAULT);
    /**
     * Flag for checking which termination factor to use:
     * basic, rmse, l2norm
     **/
    String factorFlag = getContext().getConfiguration().get(HALT_FACTOR,
      HALT_FACTOR_DEFAULT);
    /** Flag for checking if delta caching is enabled */
    boolean deltaFlag = getContext().getConfiguration().getBoolean(
      DELTA_CACHING, DELTA_CACHING_DEFAULT);
    /** Set the number of iterations */
    int iterations = getContext().getConfiguration().getInt(PARAM_ITERATIONS,
      ITERATIONS_DEFAULT);
    /** Set the Convergence Tolerance */
    float tolerance = getContext().getConfiguration()
      .getFloat(PARAM_TOLERANCE, TOLERANCE_DEFAULT);

    /* First superstep for users (superstep: 0) & items (superstep: 1) */
    if (getSuperstep() < 2) {
      initLatentVector();
    }
    /* Set flag for items */
    if (getSuperstep() == 1) {
      item = true;
    }

    /*
     * System.out.println("****  Vertex: "+getId()+", superstep:"+getSuperstep
     * ()+", item:" + item + ", " + getValue().getLatentVector());
     */
    rmseErr = 0d;
    boolean neighUpdated = false;
    /* For each message */
    for (MessageWrapper message : messages) {
      msgCounter++;
      /*
       * System.out.println(" [RECEIVE] from " + message.getSourceId().get() +
       * ", " + message.getMessage());
       *//**
       * First superstep for items --> Add Edges connecting to users -->
       * Store the rating given from users
       */
      if (getSuperstep() == 1) {
        observed = message.getMessage().get(message.getMessage().size() - 1)
          .get();
        DefaultEdge<IntWritable, IntWritable> edge =
          new DefaultEdge<IntWritable, IntWritable>();
        edge.setTargetVertexId(message.getSourceId());
        edge.setValue(new IntWritable((int) observed));
        // System.out.println("   Adding edge:" + edge);
        addEdge(edge);
        // Remove the last value from message - it's there for the 1st round
        message.getMessage().remove(message.getMessage().size() - 1);
      }
      /* If delta caching is enabled */
      /* For the fist round of either users or items, save their values */
      if (deltaFlag) {
        /* Create table with neighbors latent values and ids */
        if (getSuperstep() == 1 || getSuperstep() == 2) {
          getValue().setNeighborValue(message.getSourceId(),
            message.getMessage());
        }

        /* In the next rounds, update their values if necessary */
        if (getSuperstep() > 2) {
          if (updateNeighValues(getValue()
            .getNeighValue(message.getSourceId()), message.getMessage())) {
            neighUpdated = true;
          }
        }
      }
      if (!deltaFlag) {
        /*** Calculate error */
        observed = (double) getEdgeValue(message.getSourceId()).get();
        err = getError(getValue().getLatentVector(), message.getMessage(),
          observed);
        /** Change the Vertex Latent Vector based on SGD equation */
        runSgdAlgorithm(message.getMessage());
        err = getError(getValue().getLatentVector(), message.getMessage(),
          observed);
        /* If termination flag is set to RMSD or RMSD aggregator is true */
        if (factorFlag.equals("rmse") || rmseFlag) {
          rmseErr += Math.pow(err, 2);
        }
      }
    } // Eof Messages
    if (deltaFlag && getSuperstep() > 0) {
      for (Entry<IntWritable, DoubleArrayListWritable> vvertex : getValue()
        .getAllNeighValue().entrySet()) {
        /*** Calculate error */
        observed = (double) getEdgeValue(vvertex.getKey()).get();
        err = getError(getValue().getLatentVector(), vvertex.getValue(),
          observed);
        /**
         * If at least one neighbour has changed its latent vector then
         * calculation of vertex can not be avoided
         */
        if (neighUpdated) {
          /* Change the Vertex Latent Vector based on SGD equation */
          runSgdAlgorithm(vvertex.getValue());
          err = getError(getValue().getLatentVector(), vvertex.getValue(),
            observed);
          /* If termination flag is set to RMSE or RMSE aggregator is true */
          if (factorFlag.equals("rmse") || rmseFlag) {
            rmseErr += Math.pow(err, 2);
          }
        } // Eof if (neighUpdated)
      }
    } // Eof if (deltaFlag > 0f && getSuperstep() > 0)

    // If termination factor is set to basic - number of iterations
    if (factorFlag.equals("basic")) {
      haltFactor = tolerance + 1;
    }
    /* If RMSE aggregator flag is true */
    if (rmseFlag) {
      this.aggregate(RMSE_AGG, new DoubleWritable(rmseErr));
    }
    /* If termination factor is set to RMSE */
    if (factorFlag.equals("rmse")) {
      haltFactor = getRMSE(msgCounter);
      //System.out.println("myRMSD: " + haltFactor);
    }
    /* If termination factor is set to L2NOrm */
    if (factorFlag.equals("l2norm")) {
      haltFactor = getL2Norm(initialValue, getValue().getLatentVector());
      // System.out.println("NormVector: sqrt((initial[0]-final[0])^2 " +
      //"+ (initial[1]-final[1])^2): "
      // + err_factor);
    }
    if (getSuperstep() == 0 ||
      (haltFactor > tolerance && getSuperstep() < iterations)) {
      sendMsgs();
    }
    // err_factor is used in the OutputFormat file. --> To print the error
    if (factorFlag.equals("basic")) {
      haltFactor = err;
    }
    voteToHalt();
  } // EofCompute

  /**
   * Return type of current vertex
   *
   * @return item
   */
  public boolean isItem() {
    return item;
  }

  /*** Initialize Vertex Latent Vector */
  public void initLatentVector() {
    DoubleArrayListHashMapWritable value =
      new DoubleArrayListHashMapWritable();
    for (int i = 0; i < VECTOR_SIZE; i++) {
      value.setLatentVector(i, new DoubleWritable(
        ((double) (getId().get() + i) % 100d) / 100d));
    }
    setValue(value);
    // System.out.println("[INIT] value: " + value.getLatentVector());
    /* For L2Norm */
    initialValue = getValue().getLatentVector();
  }

  /**
   * Modify Vertex Latent Vector based on SGD equation
   *
   * @param vvertex Vertex value
   */
  public void runSgdAlgorithm(DoubleArrayListWritable vvertex) {
    /**
     * vertex_vector = vertex_vector + 2*GAMMA*(real_value -
     * dot_product(vertex_vector,other_vertex_vector))*other_vertex_vector +
     * LAMBDA * vertex_vector
     */
    DoubleArrayListWritable la = new DoubleArrayListWritable();
    DoubleArrayListWritable ra = new DoubleArrayListWritable();
    DoubleArrayListWritable ga = new DoubleArrayListWritable();
    DoubleArrayListWritable value = new DoubleArrayListWritable();
    la = numMatrixProduct((double) LAMBDA, getValue().getLatentVector());
    ra = numMatrixProduct((double) err, vvertex);
    ga = numMatrixProduct((double) -GAMMA, dotAddition(ra, la));
    value = dotAddition(getValue().getLatentVector(), ga);
    // System.out.print("Latent Vector: " + value);
    keepXdecimals(value, DECIMALS);
    // System.out.println(" , 4 decimals: " + value);
    getValue().setLatentVector(value);
    nupdates++;
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

  /**
   * Update Neighbor's values with the latest value
   *
   * @param curVal Current vertex' value
   * @param latestVal Latest vertex' value
   * @return updated True if it is updated
   */
  public boolean updateNeighValues(DoubleArrayListWritable curVal,
    DoubleArrayListWritable latestVal) {
    boolean updated = false;
    for (int i = 0; i < VECTOR_SIZE; i++) {
      if (latestVal.get(i) != curVal.get(i)) {
        curVal.set(i, latestVal.get(i));
        updated = true;
      } /* else {
        System.out.println("[COMPARE]" + curVal.get(i) + ", " +
          latestVal.get(i));
      } */
    }
    return updated;
  }

  /*** Send messages to neighbours */
  public void sendMsgs() {
    /* Create a message and wrap together the source id and the message */
    MessageWrapper message = new MessageWrapper();
    message.setSourceId(getId());

    if (getSuperstep() == 0) {
      for (Edge<IntWritable, IntWritable> edge : getEdges()) {
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
  public double getRMSE(int msgCounter) {
    return Math.sqrt(rmseErr / msgCounter);
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
    //System.out.println("L2norm: " + result);
    return Math.sqrt(result);
  }

  /**
   * Calculate the error: e=observed-predicted
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @param observed Observed value
   * @return Result from deducting observed value from predicted
   */
  public double getError(DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB, double observed) {
    /*** Predicted value */
    //System.out.println("vectorA, vectorB");
    vectorA.print();
    vectorB.print();

    double predicted = dotProduct(vectorA, vectorB);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);
    return predicted - observed;
  }

  /**
   * Calculate the dot product of 2 vectors: vector1*vector2
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return Result from dot product of 2 vectors
   */
  public double dotProduct(DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB) {
    double result = 0d;
    for (int i = 0; i < VECTOR_SIZE; i++) {
      result += vectorA.get(i).get() * vectorB.get(i).get();
    }
    return result;
  }

  /**
   * Calculate the dot addition of 2 vectors: vectorA+vectorB
   *
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return result Result from dot addition of the two vectors
   */
  public DoubleArrayListWritable dotAddition(
    DoubleArrayListWritable vectorA,
    DoubleArrayListWritable vectorB) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < VECTOR_SIZE; i++) {
      result.add(new DoubleWritable
      (vectorA.get(i).get() + vectorB.get(i).get()));
    }
    return result;
  }

  /**
   * Calculate the product num*matirx
   *
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   * */
  public DoubleArrayListWritable numMatrixProduct(double num,
    DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < VECTOR_SIZE; i++) {
      result.add(new DoubleWritable(num * matrix.get(i).get()));
    }
    return result;
  }

  /**
   * Return amount of vertex updates
   *
   * @return nupdates
   * */
  public int getUpdates() {
    return nupdates;
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
      /** Set the Convergence Tolerance */
      float tolerance = getContext().getConfiguration()
        .getFloat(PARAM_TOLERANCE, TOLERANCE_DEFAULT);
      
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
        System.out.println("SS:" + getSuperstep() + ", Total RMSE: "
          + totalRMSE);
        getAggregatedValue(RMSE_AGG);
        if (totalRMSE < tolerance) {
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
