package es.tid.graphlib.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * A Writable implementation for a HashMap element
 * 
 * @param <U>
 *          Type of the first element - HashMap
 */

public class IntIntHashMapWritable implements Writable {
  private HashMap<IntWritable, IntWritable> verticesPerPartition;

  /** Simple Constructor */
  public IntIntHashMapWritable() {
    verticesPerPartition = new HashMap<IntWritable, IntWritable>();
  }

  /** Constructor with addition of the first pair */
  public IntIntHashMapWritable(IntWritable key, IntWritable value){
    verticesPerPartition = new HashMap<IntWritable, IntWritable>();
    verticesPerPartition.put(key, value);
  }
  
  /** Write bytes */
  public void write(DataOutput output) throws IOException {
    output.writeInt(getSize());
    for (IntWritable key : verticesPerPartition.keySet()) {
      key.write(output);
      verticesPerPartition.get(key).write(output);
    }
  }

  /** Read bytes */
  public void readFields(DataInput input) throws IOException {
    verticesPerPartition = new HashMap<IntWritable, IntWritable>();
    int size = input.readInt();
    for (int i = 0; i < size; i++) {
      IntWritable key = new IntWritable();
      key.readFields(input);
      IntWritable value = new IntWritable();
      value.readFields(input);
      verticesPerPartition.put(key, value);
    }
  }

  /**
   * Add an element to the HashMap.
   * 
   * @param key
   *          Key of the pair
   * @param value
   *          Value of the pair
   */
  public void setValue(IntWritable key,
    IntWritable value) {
    verticesPerPartition.put(key, value);
  }

  /**
   * Get value
   * 
   * @param key
   * @return value
   */
  public IntWritable getValue(IntWritable key) {
    return verticesPerPartition.get(key);
  }

  /**
   * Get the whole HashMap
   * 
   * @return HashMap
   */
  public HashMap<IntWritable, IntWritable> getAllValue() {
    return verticesPerPartition;
  }

  /**
   * Get size of HashMap
   * 
   * @return Number of elements in HashMap
   */
  public int getSize() {
    return verticesPerPartition.size();
  }

  /**
   * Check if the HashMap is empty.
   * 
   * @return True iff there are no pairs in the HashMap
   */
  public boolean isEmpty() {
    return getSize() == 0;
  }
}
