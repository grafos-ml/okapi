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
  /** Baseline Estimate */
  private DoubleWritable baselineEstimate;
  /** Relative Value */
  private DoubleArrayListWritable relativeValue;

  /** Constructor */
  public SvdMessageWrapper() {
    super();
  }

  /**
   * Constructor.
   * @param sourceId Vertex Source Id
   * @param message Message
   * @param relativeValue Relative Value
   */
  public SvdMessageWrapper(IntWritable sourceId,
      DoubleArrayListWritable message, DoubleWritable baselineEstimate,
      DoubleArrayListWritable relativeValue) {
    super(sourceId, message);
    this.baselineEstimate = baselineEstimate;
    this.relativeValue = relativeValue;
  }

  /**
   * Return Baseline Estimate
   *
   * @return baselineEstimate Baseline
   */
  public DoubleWritable getBaselineEstimate() {
    return baselineEstimate;
  }

  /**
   * Set Baseline Estimate
   *
   * @param baselineEstimate
   */
  public void setBaselineEstimate(DoubleWritable baselineEstimate) {
    this.baselineEstimate = baselineEstimate;
  }

  /**
   * Return Relative Value
   *
   * @return relative Value Relative Value
   */
  public DoubleArrayListWritable getRelativeValue() {
    return relativeValue;
  }

  /**
   * Set Relative Value
   *
   * @param relativeValue Relative value
   */
  public void setRelativeValue(DoubleArrayListWritable relativeValue) {
    this.relativeValue = relativeValue;
  }

  /**
   * Read Fields
   *
   * @param input Input
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    baselineEstimate.readFields(input);
    relativeValue.readFields(input);
  }

  /**
   * Write Fields
   *
   * @param output Output
   */
  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    baselineEstimate.write(output);
    relativeValue.write(output);
  }

  /**
   * Return Message to the form of a String
   *
   * @return String object
   */
  @Override
  public String toString() {
    return "MessageWrapper{" +
      ", sourceId=" + super.getSourceId() +
      ", message=" + super.getMessage() +
      ", baseline=" + baselineEstimate +
      ", relative=" + relativeValue +
      '}';
  }
}
