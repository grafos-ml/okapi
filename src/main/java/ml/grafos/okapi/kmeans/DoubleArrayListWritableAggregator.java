package ml.grafos.okapi.kmeans;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayListWritableAggregator extends BasicAggregator<DoubleArrayListWritable> {

	@Override
	public void aggregate(DoubleArrayListWritable other) {
		DoubleArrayListWritable aggrValue = getAggregatedValue();
		if ( aggrValue.size() == 0 ) {
			// first-time creation
			for ( int i = 0; i < other.size(); i ++ ) {
				aggrValue.add(other.get(i));
			}
			setAggregatedValue(aggrValue);
		}
		else if ( aggrValue.size() < other.size() ) {
			throw new IndexOutOfBoundsException("The value to be aggregated " +
					"cannot have larger size than the aggregator value");
		}
		else {
			for ( int i = 0; i < other.size(); i ++ ) {
				DoubleWritable element = new DoubleWritable(aggrValue.get(i).get() + other.get(i).get());
				aggrValue.set(i, element);
			}
			setAggregatedValue(aggrValue);
		}
	}

	@Override
	public DoubleArrayListWritable createInitialValue() {
		return new DoubleArrayListWritable();
	}
	
}
