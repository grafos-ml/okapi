package es.tid.graphlib.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * A Writable implementation for a pair with 2 int elements
 * 
 * @param <U>
 *          Type of the first element - int
 * @param <V>
 *          Type of the second element - int
 */

public class IntPairWritable implements Writable {
  private IntPair pair;

  /** Write bytes */
  public void write(DataOutput output) throws IOException {
    output.writeInt(pair.getFirst());
    output.writeInt(pair.getSecond());
  }

  /** Read bytes */
  public void readFields(DataInput input) throws IOException {
    pair.setFirst(input.readInt());
    pair.setSecond(input.readInt());
  }

  /**
   * Set values for the pair.
   * 
   * @param fst
   *          value for first element of pair
   * @param snd
   *          value for second element of pair
   */
  public void setPair(IntWritable fst, IntWritable snd) {
    pair.setFirst(fst.get());
    pair.setSecond(snd.get());
  }

  /**
   * Get second element if first element is the one wanted
   * 
   * @param fst desired value for first element
   * @return second value of second element
   */
  public IntWritable getSecond(IntWritable fst) {
    if (fst.get() == pair.getFirst()) {
      return new IntWritable(pair.getSecond());
    }
    return null;
  }

  /**
   * Get the pair
   * 
   * @return Pair
   */
  public IntPair getPair() {
    return pair;
    
  }
}
