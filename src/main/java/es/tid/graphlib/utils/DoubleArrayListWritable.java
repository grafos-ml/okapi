package es.tid.graphlib.utils;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayListWritable
  extends ArrayListWritable<DoubleWritable> {

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
   * public int compareTo(MessageWrapper wrapper) { if (this == wrapper )
   * return 0;
   *
   * if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) return
   * this.message.compareTo(wrapper.getMessage()); else return
   * this.sourceId.compareTo(wrapper.getSourceId()); }
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
   * Compare function -- NOT DONE
   *
   * @param message Message to be compared
   *
   * @return 0
   */
  public int compareTo(DoubleArrayListWritable message) {
    if (message==null) {
      return 1;
    }
    if (this.size()<message.size()) {
      return -1;
    }
    if (this.size()>message.size()) {
      return 1;
    }
    for (int i=0; i<this.size(); i++) {
      if (this.get(i)==null && message.get(i)==null) {
        continue;
      }
      if (this.get(i)==null) {
        return -1;
      }
      if (message.get(i)==null) {
        return 1;
      }
      if (this.get(i).get()<message.get(i).get()) {
        return -1;
      }
      if (this.get(i).get()>message.get(i).get()) {
        return 1;
      }
    }
    /*
     * DoubleArrayListWritable msg = new DoubleArrayListWritable; msg = this;
     *
     * int i=0; while (i<message.size()){ if
     * (msg.toArray().equals(message.toArray())) return 0; if
     * (msg[i]>message[i]) return 1; i++; }
     */
    return 0;
  }
}
