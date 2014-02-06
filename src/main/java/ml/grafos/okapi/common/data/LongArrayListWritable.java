package ml.grafos.okapi.common.data;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class LongArrayListWritable
  extends ArrayListWritable<LongWritable>
  implements WritableComparable<LongArrayListWritable> {
  /** Default constructor for reflection */
  public LongArrayListWritable() {
    super();
  }

  @Override
  public void setClass() {
    setClass(LongWritable.class);
  }

  /**
   * public int compareTo(MessageWrapper wrapper) { if (this == wrapper )
   * return 0;
   *
   * if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) return
   * this.message.compareTo(wrapper.getMessage()); 
   * else return this.sourceId.compareTo(wrapper.getSourceId()); }
   *
   * @param message Message to be compared
   * @return 0 value
   */
  @Override
  public int compareTo(LongArrayListWritable message) {
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
    return 0;
  }
}
