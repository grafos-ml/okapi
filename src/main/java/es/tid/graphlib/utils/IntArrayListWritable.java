package es.tid.graphlib.utils;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntArrayListWritable
  extends ArrayListWritable<IntWritable>
  implements WritableComparable {
  /** Default constructor for reflection */
  public IntArrayListWritable() {
    super();
  }

  @Override
  public void setClass() {
    setClass(IntWritable.class);
  }

  /**
   * public int compareTo(MessageWrapper wrapper) { if (this == wrapper )
   * return 0;
   *
   * if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) return
   * this.message.compareTo(wrapper.getMessage()); else return
   * this.sourceId.compareTo(wrapper.getSourceId()); }
   *
   * @param message Message to be compared
   * @return 0 value
   */
  public int compareTo(IntArrayListWritable message) {
    /*
     * DoubleArrayListWritable msg = new DoubleArrayListWritable; msg = this;
     *
     * int i=0; while (i<message.size()){ if
     * (msg.toArray().equals(message.toArray())) return 0; if
     * (msg[i]>message[i]) return 1; i++; }
     */
    return 0;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }
}
