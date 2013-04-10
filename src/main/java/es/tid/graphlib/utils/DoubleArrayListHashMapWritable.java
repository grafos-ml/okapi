package es.tid.graphlib.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;



/**
 * A Writable implementation for 2 elements
 * @param <U> Type of the first element
 * @param <V> Type of the second element - HashMap
 *
 */

public class DoubleArrayListHashMapWritable implements Writable {

	private DoubleArrayListWritable sourceValue;
	private HashMap<IntWritable, DoubleArrayListWritable> neighValues;

	/** Constructor */
	public DoubleArrayListHashMapWritable(){
		sourceValue = new DoubleArrayListWritable();
		neighValues = new HashMap<IntWritable, DoubleArrayListWritable>();
	}
	
	/** Write bytes */
	public void write(DataOutput output) throws IOException {
		sourceValue.write(output);
		output.writeInt(getSize());
	    for (IntWritable key : neighValues.keySet()) {
	    	key.write(output);
	    	neighValues.get(key).write(output);
	    }
	}
	
	/** Read bytes */
	public void readFields(DataInput input) throws IOException {
	    sourceValue = new DoubleArrayListWritable();
		sourceValue.readFields(input);
		
		neighValues = new HashMap<IntWritable, DoubleArrayListWritable>();
		int size = input.readInt();
		for (int i=0; i<size; i++) {
	    	IntWritable key = new IntWritable();
	    	key.readFields(input);
	    	DoubleArrayListWritable value = new DoubleArrayListWritable();
	    	value.readFields(input);
	    	neighValues.put(key, value);
	    }
	}
	
	 /**
	   * Set vertex latent value
	   *
	   * @param Vertex Latent Value 
	   */
	  public void setLatentVector(DoubleArrayListWritable value) {
	    sourceValue = value;
	  }
	  
	  /**
	   * Set one element of vertex latent value
	   *
	   * @param index index of the vertex Latent vector
	   * @param value Latent Value for the index
	   */
	  public void setLatentVector(int index, DoubleWritable value){
		  sourceValue.add(index, value);
	  }
	  
	  /**
	   * Get vertex latent vector values
	   * 
	   * @return Vertex Latent Value
	   */
	  public DoubleArrayListWritable getLatentVector() {
		  return sourceValue;
	  }
	 
	 /**
	   * Add a vertex latent value to the HashMap.
	   *
	   * @param neighId Key of the pair
	   * @param neighVal Value of the pair
	   */
	  public void setNeighborValue(IntWritable neighId, DoubleArrayListWritable neighVal) {
	    neighValues.put(neighId, neighVal);
	  }
	
	  /**
	   * Get vertex neighbors latent vector values
	   * 
	   * @param Neighbor Id
	   * @return Neighbor Latent Value
	   */
	  public DoubleArrayListWritable getNeighValue(IntWritable id){
		  return neighValues.get(id);
	  }
	  
	  /**
	   * Get the neighbors vertices values (data stored with vertex)
	   *
	   * @return Neighbors values
	   */
	  public HashMap<IntWritable,DoubleArrayListWritable>getAllNeighValue() {
		  return neighValues;
	  }
	  
	 /**
	   * Get number of neighbors latent vectors in this list.
	   *
	   * @return Number of neighbors latent vectors in the list
	   */
	  public int getSize() {
	    return neighValues.size();
	  }
	  
	  /**
	   * Check if the list is empty.
	   *
	   * @return True iff there are no pairs in the list
	   */
	  public boolean isEmpty() {
	    return getSize() == 0;
	  }
}
