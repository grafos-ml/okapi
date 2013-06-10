package es.tid.graphlib.utils;

import org.apache.giraph.utils.IntPair;

/**
 * A pair of integers and the value on their edge.
 */
public class IntPairDoubleVal extends IntPair {
  /** Value between two elements */
  private double value;

  /**
   * Constructor.
   *
   * @param fst First element
   * @param snd Second element
   * @param val Edge Value
   */
  public IntPairDoubleVal(int fst, int snd, double val) {
    super(fst, snd);
    value = val;
  }

  /**
   * Get the value.
   *
   * @return The value
   */
  public double getValue() {
    return value;
  }

  /**
   * Set the value between two elements.
   *
   * @param value Value
   */
  public void setValue(double value) {
    this.value = value;
  }
}
