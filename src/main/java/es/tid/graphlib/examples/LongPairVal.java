package es.tid.graphlib.examples;


public class LongPairVal extends LongPair {
	/** Value between two elements. */
	private double value;
	
	/** Constructor.
	  *
	  * @param fst First element
	  * @param snd Second element
	  * @param val Value
	  */
	public LongPairVal(long fst, long snd, double val) {
		super(fst, snd);
	    value = val;
	}

	  /**
	   * Get the value.
	   *
	   * @return The value
	   */
	  public double getValue(){
		  return value;
	  }
	  
	  /**
	   * Set the value between two elements.
	   *
	   * @param value The value
	   */
	  public void setValue(double value) {
		  this.value = value;
	  }
	  
}
