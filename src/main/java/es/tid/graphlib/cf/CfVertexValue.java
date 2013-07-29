package es.tid.graphlib.cf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import es.tid.graphlib.utils.DoubleArrayListWritable;

/**
 * A Writable implementation of a DoubleArrayList.
 * First element: latentVector
 */

public class CfVertexValue
implements Writable {

  /** For serialization. */
  private static final long serialVersionUID = 1L;
  /** Source Vertex Latent Vector. */
  private DoubleArrayListWritable latentVector;

  /** Constructor. */
  public CfVertexValue() {
    latentVector = new DoubleArrayListWritable();
  }

  /**
   * Write bytes.
   *
   * @param output Output
   * @throws IOException for IO
   */
  public void write(final DataOutput output) throws IOException {
    latentVector.write(output);
  }

  /**
   * Read bytes.
   *
   * @param input Input
   * @throws IOException for IO
   */
  public void readFields(final DataInput input) throws IOException {
    latentVector = new DoubleArrayListWritable();
    latentVector.readFields(input);
  }

  /**
   * Set vertex latent value.
   *
   * @param value Vertex Latent Value
   */
  public final void setLatentVector(final DoubleArrayListWritable value) {
    latentVector = value;
  }

  /**
   * Set one element of vertex latent value.
   *
   * @param index Index of the vertex Latent vector
   * @param value Latent Value for the index
   */
  public final void setLatentVector(final int index,
    final DoubleWritable value) {
    latentVector.add(index, value);
  }

  /**
   * Get vertex latent vector values.
   *
   * @return Vertex Latent Value
   */
  public final DoubleArrayListWritable getLatentVector() {
    return latentVector;
  }
}
