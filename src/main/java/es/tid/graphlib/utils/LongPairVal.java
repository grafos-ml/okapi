package es.tid.graphlib.utils;

/**
 * A pair of long numbers and the value on their edge.
 */
public class LongPairVal extends LongPair {
  /** Value between two elements. */
  private float value;

  /**
   * Constructor.
   *
   * @param fst First element
   * @param snd Second element
   * @param val Value
   */
  public LongPairVal(long fst, long snd, float val) {
    super(fst, snd);
    value = val;
  }

  /**
   * Get the value.
   *
   * @return value Value between two elements
   */
  public float getValue() {
    return value;
  }

  /**
   * Set the value between two elements.
   *
   * @param value Value to be set
   */
  public void setValue(float value) {
    this.value = value;
  }
}
