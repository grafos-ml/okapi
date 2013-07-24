package es.tid.graphlib.svd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

/** This class provides the wrapper for the sending message.*/
public class SvdMessageWrapper extends MessageWrapper {
  /** Baseline Estimate. */
  private DoubleWritable baselineEstimate;
  /** Relative Value. */
  private DoubleArrayListWritable relativeValue;
  /** Number of user's  neighbours (outgoing edges). */
  private IntWritable numEdges;

  /** Constructor. */
  public SvdMessageWrapper() {
    super();
  }

  /**
   * Constructor.
   * @param sourceId Vertex Source Id
   * @param message Message
   * @param pBaselineEstimate Baseline Estimate
   * @param pRelativeValue Relative Value
   * @param pNumEdges Number of Edges
   */
  public SvdMessageWrapper(final IntWritable sourceId,
      final DoubleArrayListWritable message,
      final DoubleWritable pBaselineEstimate,
      final DoubleArrayListWritable pRelativeValue,
      final IntWritable pNumEdges) {
    super(sourceId, message);
    baselineEstimate = pBaselineEstimate;
    relativeValue = pRelativeValue;
    numEdges = pNumEdges;
  }

  /**
   * Return Baseline Estimate.
   *
   * @return baselineEstimate Baseline
   */
  public final DoubleWritable getBaselineEstimate() {
    return baselineEstimate;
  }

  /**
   * Set Baseline Estimate.
   *
   * @param pBaselineEstimate Baseline Estimate
   */
  public final void setBaselineEstimate(
    final DoubleWritable pBaselineEstimate) {
    this.baselineEstimate = pBaselineEstimate;
  }

  /**
   * Set Relative Value.
   *
   * @param pRelativeValue Relative value
   */
  public final void setRelativeValue(
    final DoubleArrayListWritable pRelativeValue) {
    this.relativeValue = pRelativeValue;
  }

  /**
   * Return Relative Value.
   *
   * @return relative Value Relative Value
   */
  public final DoubleArrayListWritable getRelativeValue() {
    return relativeValue;
  }

  /**
   * Set number of user's neighbors.
   *
   * @param pNumEdges Number of edges
   */
  public final void setNumEdges(final IntWritable pNumEdges) {
    numEdges = pNumEdges;
  }

  /**
   * Return user's neighbors.
   *
   * @return numEdges Number of edges
   */
  public final IntWritable getNumEdges() {
    return numEdges;
  }

  /**
   * Read Fields.
   *
   * @param input Input
   * @throws IOException for input
   */
  @Override
  public final void readFields(final DataInput input) throws IOException {
    super.readFields(input);
    baselineEstimate = new DoubleWritable();
    baselineEstimate.readFields(input);
    relativeValue = new DoubleArrayListWritable();
    relativeValue.readFields(input);
    numEdges = new IntWritable();
    numEdges.readFields(input);
  }

  /**
   * Write Fields.
   *
   * @param output Output
   * @throws IOException for output
   */
  @Override
  public final void write(final DataOutput output) throws IOException {
    super.write(output);
    baselineEstimate.write(output);
    relativeValue.write(output);
    numEdges.write(output);
  }

  /**
   * Return Message to the form of a String.
   *
   * @return String object
   */
  @Override
  public final String toString() {
    return "MessageWrapper{"
      + ", sourceId=" + super.getSourceId()
      + ", message=" + super.getMessage()
      + ", baseline=" + baselineEstimate
      + ", relative=" + relativeValue
      + ", numEdges=" + numEdges
      + '}';
  }
}
