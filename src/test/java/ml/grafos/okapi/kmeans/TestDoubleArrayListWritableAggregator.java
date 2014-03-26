package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

public class TestDoubleArrayListWritableAggregator {
	private static double E = 0.0001f;
	
	@Test
	public void test() {
		DoubleArrayListWritableAggregator aggr = new DoubleArrayListWritableAggregator();
		DoubleArrayListWritable other = new DoubleArrayListWritable();
		other.add(new DoubleWritable(1.0));
		other.add(new DoubleWritable(2.0));
		aggr.aggregate(other);
		assertEquals(1.0, aggr.getAggregatedValue().get(0).get(), E);
		assertEquals(2.0, aggr.getAggregatedValue().get(1).get(), E);
		aggr.aggregate(other);
		assertEquals(2.0, aggr.getAggregatedValue().get(0).get(), E);
		assertEquals(4.0, aggr.getAggregatedValue().get(1).get(), E);
	}

}
