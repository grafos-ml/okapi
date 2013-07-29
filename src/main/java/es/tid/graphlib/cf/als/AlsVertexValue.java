package es.tid.graphlib.cf.als;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import es.tid.graphlib.cf.CfVertexValue;
import es.tid.graphlib.utils.DoubleArrayListWritable;

/**
 * A Writable implementation for 2 elements.
 * First element: sourceValue
 * Second element: neighValues
 */

public class AlsVertexValue
extends CfVertexValue
implements Writable {

  /** For serialization. */
  private static final long serialVersionUID = 1L;

  /** Neighbors Values. */
  private HashMap<Text, DoubleArrayListWritable> neighValues;

  /** Constructor. */
  public AlsVertexValue() {
    super();
    neighValues = new HashMap<Text, DoubleArrayListWritable>();
  }

  /**
   * Write bytes.
   *
   * @param output Output
   * @throws IOException for IO
   */
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeInt(getSize());
    for (Text key : neighValues.keySet()) {
      key.write(output);
      neighValues.get(key).write(output);
    }
  }

  /**
   * Read bytes.
   *
   * @param input Input
   * @throws IOException for IO
   */
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);

    neighValues = new HashMap<Text, DoubleArrayListWritable>();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      Text key = new Text();
      key.readFields(input);
      DoubleArrayListWritable value = new DoubleArrayListWritable();
      value.readFields(input);
      neighValues.put(key, value);
    }
  }

  /**
   * Add a vertex latent value to the HashMap.
   *
   * @param neighId Key of the pair
   * @param neighVal Value of the pair
   */
  public final void setNeighborValue(final Text neighId,
    final DoubleArrayListWritable neighVal) {
    neighValues.put(neighId, neighVal);
  }

  /**
   * Get vertex neighbors latent vector values.
   *
   * @param id Neighbor's Id
   * @return Neighbor Latent Value
   */
  public final DoubleArrayListWritable getNeighValue(final Text id) {
    return neighValues.get(id);
  }

  /**
   * Get the neighbors vertices values (data stored with vertex).
   *
   * @return Neighbors values
   */
  public final HashMap<
    Text, DoubleArrayListWritable> getAllNeighValue() {
    return neighValues;
  }

  /**
   * Get number of neighbors latent vectors in this list.
   *
   * @return Number of neighbors latent vectors in the list
   */
  public final int getSize() {
    return neighValues.size();
  }

  /**
   * Check if the list is empty.
   *
   * @return True iff there are no pairs in the list
   */
  public final boolean isEmpty() {
    return getSize() == 0;
  }
}
