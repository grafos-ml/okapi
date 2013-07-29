package es.tid.graphlib.utils;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayListWritable
  extends ArrayListWritable<DoubleWritable>
  implements WritableComparable<DoubleArrayListWritable>{

  /**
   * Serialization number.
   */
  private static final long serialVersionUID = 5968423975161632117L;

  /** Default constructor for reflection */
  public DoubleArrayListWritable() {
    super();
  }

  /**
   * Constructor
   *
   * @param other Other
   */
  public DoubleArrayListWritable(DoubleArrayListWritable other) {
    super(other);
  }

  @Override
  public void setClass() {
    setClass(DoubleWritable.class);
  }

  /**
   * Print the Double Array List
   */
  public void print() {
    System.out.print("[");
    for (int i = 0; i < this.size(); i++) {
      System.out.print(this.get(i));
      if (i < this.size() - 1) {
        System.out.print("; ");
      }
    }
    System.out.println("]");
  }

  /**
   * Compare function
   *
   * @param message Message to be compared
   *
   * @return 0
   */
  public int compareTo(DoubleArrayListWritable message) {
    if (message==null) {
      return 1;
    }
    if (this.size() < message.size()) {
      return -1;
    }
    if (this.size() > message.size()) {
      return 1;
    }
    for (int i=0; i < this.size(); i++) {
      if (this.get(i) == null && message.get(i) == null) {
        continue;
      }
      if (this.get(i) == null) {
        return -1;
      }
      if (message.get(i) == null) {
        return 1;
      }
      if (this.get(i).get() < message.get(i).get()) {
        return -1;
      }
      if (this.get(i).get() > message.get(i).get()) {
        return 1;
      }
    }
    return 0;
  }
}
