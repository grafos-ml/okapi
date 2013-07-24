package es.tid.graphlib.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.examples.Sgd.DoubleArrayListWritable;
import es.tid.graphlib.examples.Sgd.MessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions")

public class Sgd extends Vertex<Text, DoubleArrayListWritable,
DoubleWritable, MessageWrapper> {
  /** Keyword for parameter setting the convergence tolerance parameter
   *  depending on the version enabled; l2norm or rmse */
  public static final String TOLERANCE_KEYWORD = "sgd.halting.tolerance";
  /** Default value for TOLERANCE */
  public static final float TOLERANCE_DEFAULT = 1f;
  /** Keyword for parameter setting the number of iterations */
  public static final String ITERATIONS_KEYWORD = "sgd.iterations";
  /** Default value for ITERATIONS */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Regularization parameter LAMBDA */
  public static final String LAMBDA_KEYWORD = "sgd.lambda";
  /** Default value for LABDA */
  public static final float LAMBDA_DEFAULT = 0.01f;
  /** Keyword for parameter setting the learning rate GAMMA */
  public static final String GAMMA_KEYWORD = "sgd.gamma";
  /** Default value for GAMMA */
  public static final float GAMMA_DEFAULT = 0.005f;
  /** Keyword for parameter setting the Latent Vector Size */
  public static final String VECTOR_SIZE_KEYWORD = "sgd.vector.size";
  /** Default value for GAMMA */
  public static final int VECTOR_SIZE_DEFAULT = 2;
  /** Max rating */
  public static final double MAX = 5;
  /** Min rating */
  public static final double MIN = 0;
  /** Decimals */
  public static final int DECIMALS = 4;
  /** Prefix of item IDs. Used to distinguish from users */
  private final String ITEM_ID_PREFIX = "i_";
  /** Initial vector value to be used for the L2Norm case */
  private DoubleArrayListWritable initialValue;

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<MessageWrapper> messages) {
    /** Error between predicted and observed rating */
    double err = 0d;

    /* Set the number of iterations */
    int maxIterations = getContext().getConfiguration().getInt(
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

    // Initialize vertex latent vector in the first superstep for both users
    // and items.
    if (getSuperstep() == 0) {
      initLatentVector(vectorSize);
    }
    
    // Item vertices do not do anything on the first superstep
    if (getSuperstep() == 0 && getId().toString().startsWith(ITEM_ID_PREFIX)) {
      return;
    }
    
    initialValue = getValue();

    for (MessageWrapper message : messages) {
      // Calculate error
      double observed = (double) getEdgeValue(message.getSourceId()).get();
      err = getError(getValue(), message.getMessage(), observed);
      // Change the Vertex Latent Vector based on SGD equation
      runSgdAlgorithm(message.getMessage(), lambda, gamma, err);
    }

    double diff = getL2Norm(initialValue, getValue());

    if (getSuperstep()<maxIterations) {
      // The diff is not defined for the first two supersteps 
      if (diff>tolerance || getSuperstep()<2) {
        sendMessage();
      }
    }

    voteToHalt();
  } 

  /** Initialize Vertex Latent Vector
   * @param vectorSize Latent Vector Size
   */
  public void initLatentVector(int vectorSize) {
    DoubleArrayListWritable value = new DoubleArrayListWritable();
    for (int i = 0; i < vectorSize; i++) {
      value.add(new DoubleWritable(
          ((double) (getId().toString().hashCode()+i) % 100d) / 100d));
    }
    keepXdecimals(value, DECIMALS);
    setValue(value);
  }

  /**
   * Modify Vertex Latent Vector based on SGD equation
   *
   * @param vvertex Vertex value
   * @param vectorSize Latent Vector Size
   * @param lambda regularization parameter
   * @param gamma learning rate
   */
  public void runSgdAlgorithm(
      DoubleArrayListWritable vvertex, float lambda, float gamma, double err) {
    /**
     * vertex_vector = vertex_vector + part3
     *
     * part1 = LAMBDA * vertex_vector
     * part2 = real_value - dot_product(vertex_vector,other_vertex_vector)) *
     * other_vertex_vector
     * part3 = - GAMMA * (part1 + part2)
     */    
    DoubleArrayListWritable part1 = 
        numMatrixProduct((double) lambda, getValue());
    DoubleArrayListWritable part2 = 
        numMatrixProduct((double) err, vvertex);
    DoubleArrayListWritable part3 = 
        numMatrixProduct((double) -gamma, dotAddition(part1, part2));
    DoubleArrayListWritable value = 
        dotAddition(getValue(), part3);
    keepXdecimals(value, DECIMALS);
    setValue(value);
  }

  /**
   * Decimal Precision of latent vector values
   *
   * @param vector Value to be truncated
   * @param numDecimals Number of decimals to keep
   */
  private void keepXdecimals(DoubleArrayListWritable vector, int numDecimals) {
    for (int i = 0; i < vector.size(); i++) {
      double val = vector.get(i).get();
      val = val * Math.pow(10, numDecimals-1);
      val = Math.round(val);
      val = val / Math.pow(10, numDecimals-1);
      vector.set(i, new DoubleWritable(val));
    }
  }

  /*** Send messages to neighbours */
  private void sendMessage() {
    MessageWrapper message = new MessageWrapper();
    message.setSourceId(getId());
    message.setMessage(getValue());
    sendMessageToAllEdges(message);
  }

  /**
   * Calculate the L2Norm on the initial and final value of vertex
   *
   * @param valOld Old value
   * @param valNew New value
   * @return result of L2Norm equation
   * */
  private double getL2Norm(DoubleArrayListWritable valOld,
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
  private double getError(DoubleArrayListWritable vectorA,
      DoubleArrayListWritable vectorB, double observed) {
    double predicted = dotProduct(vectorA, vectorB);
    predicted = Math.min(predicted, MAX);
    predicted = Math.max(predicted, MIN);
    return predicted - observed;
  }

  /**
   * Calculate the dot product of 2 vectors: vector1*vector2
   *
   * @param vectorSize Latent Vector Size
   * @param vectorA Vector A
   * @param vectorB Vector B
   * @return Result from dot product of 2 vectors
   */
  private double dotProduct(DoubleArrayListWritable vectorA,
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
  private DoubleArrayListWritable dotAddition(
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
   * Calculate the product num*matirx
   *
   * @param vectorSize Latent Vector Size
   * @param num Number to be multiplied with matrix
   * @param matrix Matrix to be multiplied with number
   * @return result Result from multiplication
   */
  private DoubleArrayListWritable numMatrixProduct(
      double num, DoubleArrayListWritable matrix) {
    DoubleArrayListWritable result = new DoubleArrayListWritable();
    for (int i = 0; i < matrix.size(); i++) {
      result.add(new DoubleWritable(num * matrix.get(i).get()));
    }
    return result;
  }


  /**
   * Class to hold a latent vector.
   *
   */
  public static class DoubleArrayListWritable
  extends ArrayListWritable<DoubleWritable> implements WritableComparable {

    /** Default constructor for reflection */
    public DoubleArrayListWritable() {
      super();
    }

    public DoubleArrayListWritable(DoubleArrayListWritable other) {
      super(other);
    }

    @Override
    public void setClass() {
      setClass(DoubleWritable.class);
    }

    /**
     * Print the Double Array List
     */
    public void print() {
      System.out.print("[");
      for (int i = 0; i < this.size(); i++) {
        System.out.print(this.get(i));
        if (i < this.size() - 1) {
          System.out.print("; ");
        }
      }
      System.out.println("]");
    }

    /**
     * Compare function
     *
     * @param message Message to be compared
     *
     * @return 0
     */
    @Override
    public int compareTo(Object other) {
      if (other==null) {
        return 1;
      }
      DoubleArrayListWritable message = (DoubleArrayListWritable)other;

      if (this.size() < message.size()) {
        return -1;
      }
      if (this.size() > message.size()) {
        return 1;
      }
      for (int i=0; i < this.size(); i++) {
        if (this.get(i) == null && message.get(i) == null) {
          continue;
        }
        if (this.get(i) == null) {
          return -1;
        }
        if (message.get(i) == null) {
          return 1;
        }
        if (this.get(i).get() < message.get(i).get()) {
          return -1;
        }
        if (this.get(i).get() > message.get(i).get()) {
          return 1;
        }
      }
      return 0;
    }
  }


  public static class MessageWrapper 
  implements WritableComparable<MessageWrapper> {
    /** Message sender vertex Id */
    private Text sourceId;
    /** Message with data */
    private DoubleArrayListWritable message;

    /** Constructor */
    public MessageWrapper() {
    }

    /**
     * Constructor.
     * @param sourceId Vertex Source Id
     * @param message Message
     */
    public MessageWrapper(Text sourceId,
        DoubleArrayListWritable message) {
      this.sourceId = sourceId;
      this.message = message;
    }

    /**
     * Return Vertex Source Id
     *
     * @return sourceId Message sender vertex Id
     */
    public Text getSourceId() {
      return sourceId;
    }

    public void setSourceId(Text sourceId) {
      this.sourceId = sourceId;
    }

    /**
     * Return Message data
     *
     * @return message message to be returned
     */
    public DoubleArrayListWritable getMessage() {
      return message;
    }

    /**
     * Store message to this object
     *
     * @param message Message to be stored
     */
    public void setMessage(DoubleArrayListWritable message) {
      this.message = message;
    }

    /**
     * Read Fields
     *
     * @param input Input
     */
    @Override
    public void readFields(DataInput input) throws IOException {
      sourceId = new Text();
      sourceId.readFields(input);
      message = new DoubleArrayListWritable();
      message.readFields(input);
    }

    /**
     * Write Fields
     *
     * @param output Output
     */
    @Override
    public void write(DataOutput output) throws IOException {
      if (sourceId == null) {
        throw new IllegalStateException("write: Null destination vertex index");
      }
      sourceId.write(output);
      message.write(output);
    }

    /**
     * Return Message to the form of a String
     *
     * @return String object
     */
    @Override
    public String toString() {
      return "MessageWrapper{" +
          ", sourceId=" + sourceId +
          ", message=" + message +
          '}';
    }

    /**
     * Check if object is equal to message
     *
     * @param o Object to be checked
     *
     * @return boolean value
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MessageWrapper that = (MessageWrapper) o;

      if (message != null ? !message.equals(that.message) :
        that.message != null) {
        return false;
      }
      if (sourceId != null ? !sourceId.equals(that.sourceId) :
        that.sourceId != null) {
        return false;
      }
      return true;
    }

    /**
     * CompareTo method
     *
     * @param wrapper Wrapper to be compared to
     *
     * @return 0 if equal
     */
    @Override
    public int compareTo(MessageWrapper wrapper) {

      if (this == wrapper) {
        return 0;
      }

      if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) {
        return this.message.compareTo(wrapper.getMessage());
      } else {
        return this.sourceId.compareTo(wrapper.getSourceId());
      }
    }
  }
}
