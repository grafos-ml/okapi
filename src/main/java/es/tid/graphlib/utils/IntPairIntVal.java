package es.tid.graphlib.utils;

import org.apache.giraph.utils.IntPair;

/**
 * A pair of integers and the value on their edge.
 */
public class IntPairIntVal extends IntPair {
  /** Value between two elements */
  private int value;

  /**
   * Constructor.
   *
   * @param fst First element
   * @param snd Second element
   * @param val Edge Value
   */
  public IntPairIntVal(int fst, int snd, int val) {
    super(fst, snd);
    value = val;
  }

  /**
   * Get the value.
   *
   * @return The value
   */
  public int getValue() {
    return value;
  }

  /**
   * Set the value between two elements.
   *
   * @param value Value
   */
  public void setValue(int value) {
    this.value = value;
  }
}
