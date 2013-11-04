package ml.grafos.okapi.cf.svd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.cf.CfVertexValue;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.hadoop.io.DoubleWritable;


/**
 * A Writable extension of the DoubleArrayListHashMapWritable.
 *
 * It inherits 1 element
 * Element: latentVector
 *
 * And it implements 2 new elements
 * Third element: baselineEstimate
 * Fourth element: relativeValue
 */

public class SvdppVertexValue
extends CfVertexValue {
  /** Observed Deviation. */
  private DoubleWritable baselineEstimate;
  /** Relative Value. */
  private DoubleArrayListWritable relativeValue;

  /** Constructor. */
  public SvdppVertexValue() {
    super();
    baselineEstimate = new DoubleWritable();
    relativeValue = new DoubleArrayListWritable();
  }

  /**
   * Write bytes.
   *
   * @param output Output
   * @throws IOException for the output
   */
  public final void write(final DataOutput output) throws IOException {
    super.write(output);
    baselineEstimate.write(output);
    output.writeInt(getSize());
    relativeValue.write(output);
  }

  /**
   * Read bytes.
   *
   * @param input Input
   * @throws IOException for the input
   */
  public final void readFields(final DataInput input) throws IOException {
    super.readFields(input);
    baselineEstimate = new DoubleWritable();
    relativeValue = new DoubleArrayListWritable();
    baselineEstimate.readFields(input);
    relativeValue.readFields(input);
  }

  /**
   * Get number of neighbors latent vectors in this list.
   *
   * @return Number of neighbors latent vectors in the list
   */
  public int getSize() {
    return relativeValue.size();
  }

  /**
   * Set baseline estimate.
   *
   * @param value Baseline estimate
   */
  public final void setBaselineEstimate(final DoubleWritable value) {
    baselineEstimate = value;
  }


  /**
   * Get baseline estimate.
   *
   * @return Vertex Baseline Estimate
   */
  public final DoubleWritable getBaselineEstimate() {
    return baselineEstimate;
  }

  /**
   * Set array relative value.
   *
   * @param value Relative Value
   */
  public final void setRelativeValue(final DoubleArrayListWritable value) {
    relativeValue = value;
  }

  /**
   * Set array relative value.
   *
   * @param index Index of the vertex Latent vector
   * @param value Relative Value
   */
  public final void setRelativeValue(final int index,
    final DoubleWritable value) {
    relativeValue.add(index, value);
  }

  /**
   * Get relative value.
   *
   * @return Vertex Relative Value
   */
  public final DoubleArrayListWritable getRelativeValue() {
    return relativeValue;
  }

  /**
   * Return Message to the form of a String.
   *
   * @return String object
   */
  @Override
  public final String toString() {
    return "VertexValue{"
      + "value=" + super.getLatentVector()
      + ", baseline=" + baselineEstimate
      + ", relative=" + relativeValue
      + '}';
  }
}
