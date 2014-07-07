package ml.grafos.okapi.common.data;

import org.apache.giraph.utils.ArrayListWritable;

public class ArrayListOfDoubleArrayListWritable extends ArrayListWritable<DoubleArrayListWritable> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1547484036891955472L;

	@Override
	public void setClass() {
		setClass(DoubleArrayListWritable.class);
	}


}
