package ml.grafos.okapi.kmeans;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;

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
