package es.tid.graphlib.utils;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;

@SuppressWarnings("serial")
public class DoubleArrayListWritable
	extends ArrayListWritable<DoubleWritable>{	
	/** Default constructor for reflection */
	public DoubleArrayListWritable() {
		super();
	}

	public DoubleArrayListWritable(DoubleArrayListWritable other) {
		super(other);
	}
	
	@Override
	public void setClass() {
		setClass(DoubleWritable.class);
	}
	
	/*
	public int compareTo(MessageWrapper wrapper) {
		if (this == wrapper )
			return 0;

		if (this.sourceId.compareTo(wrapper.getSourceId()) == 0)
			return this.message.compareTo(wrapper.getMessage());
		else
			return this.sourceId.compareTo(wrapper.getSourceId());
	}
	*/
	public int compareTo(DoubleArrayListWritable message) {
		/*
		DoubleArrayListWritable msg = new DoubleArrayListWritable;
		msg = this;
		
		int i=0;
		while (i<message.size()){
			if (msg.toArray().equals(message.toArray()))
				return 0;
			if (msg[i]>message[i])
				return 1;
			i++;
		}*/
		return 0;
	}
}