package es.tid.graphlib.cf.als;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import es.tid.graphlib.utils.DoubleArrayListWritable;

/**
 * A Writable implementation for 2 elements.
 * First element: sourceValue
 * Second element: neighValues
 */

public class AlsVertexValueType
extends DoubleArrayListWritable
implements Writable {

  /** For serialization. */
  private static final long serialVersionUID = 1L;
  /** Source Vertex Value. */
  private DoubleArrayListWritable sourceValue;
  /** Neighbors Values. */
  private HashMap<IntWritable, DoubleArrayListWritable> neighValues;

  /** Constructor. */
  public AlsVertexValueType() {
    sourceValue = new DoubleArrayListWritable();
    neighValues = new HashMap<IntWritable, DoubleArrayListWritable>();
  }

  /**
   * Write bytes.
   *
   * @param output Output
   * @throws IOException for IO
   */
  public final void write(final DataOutput output) throws IOException {
    sourceValue.write(output);
    output.writeInt(getSize());
    for (IntWritable key : neighValues.keySet()) {
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
  public final void readFields(final DataInput input) throws IOException {
    sourceValue = new DoubleArrayListWritable();
    sourceValue.readFields(input);

    neighValues = new HashMap<IntWritable, DoubleArrayListWritable>();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      IntWritable key = new IntWritable();
      key.readFields(input);
      DoubleArrayListWritable value = new DoubleArrayListWritable();
      value.readFields(input);
      neighValues.put(key, value);
    }
  }

  /**
   * Set vertex latent value.
   *
   * @param value Vertex Latent Value
   */
  public final void setLatentVector(final DoubleArrayListWritable value) {
    sourceValue = value;
  }

  /**
   * Set one element of vertex latent value.
   *
   * @param index Index of the vertex Latent vector
   * @param value Latent Value for the index
   */
  public final void setLatentVector(final int index,
    final DoubleWritable value) {
    sourceValue.add(index, value);
  }

  /**
   * Get vertex latent vector values.
   *
   * @return Vertex Latent Value
   */
  public final DoubleArrayListWritable getLatentVector() {
    return sourceValue;
  }

  /**
   * Add a vertex latent value to the HashMap.
   *
   * @param neighId Key of the pair
   * @param neighVal Value of the pair
   */
  public final void setNeighborValue(final IntWritable neighId,
    final DoubleArrayListWritable neighVal) {
    neighValues.put(neighId, neighVal);
  }

  /**
   * Get vertex neighbors latent vector values.
   *
   * @param id Neighbor's Id
   * @return Neighbor Latent Value
   */
  public final DoubleArrayListWritable getNeighValue(final IntWritable id) {
    return neighValues.get(id);
  }

  /**
   * Get the neighbors vertices values (data stored with vertex).
   *
   * @return Neighbors values
   */
  public final HashMap<
    IntWritable, DoubleArrayListWritable> getAllNeighValue() {
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
