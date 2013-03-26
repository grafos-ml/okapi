package es.tid.graphlib.examples;

/**
 * A pair of integers.
 */
public class LongPair {
  /** First element. */
  private long first;
  /** Second element. */
  private long second;

  /** Constructor.
   *
   * @param fst First element
   * @param snd Second element
   */
  public LongPair(long fst, long snd) {
    first = fst;
    second = snd;
  }

  /**
   * Get the first element.
   *
   * @return The first element
   */
  public long getFirst() {
    return first;
  }

  /**
   * Set the first element.
   *
   * @param first The first element
   */
  public void setFirst(long first) {
    this.first = first;
  }

  /**
   * Get the second element.
   *
   * @return The second element
   */
  public long getSecond() {
    return second;
  }

  /**
   * Set the second element.
   *
   * @param second The second element
   */
  public void setSecond(long second) {
    this.second = second;
  }
}
