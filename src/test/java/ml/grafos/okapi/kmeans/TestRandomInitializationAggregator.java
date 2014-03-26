package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

public class TestRandomInitializationAggregator {
	private static double E = 0.0001f;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void test() {
		
		GiraphConfiguration conf = new GiraphConfiguration();
		ImmutableClassesGiraphConfiguration immConf = new ImmutableClassesGiraphConfiguration(conf);
		immConf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		immConf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 7);
		
		ArrayListOfDoubleArrayListWritableAggregator aggr = new ArrayListOfDoubleArrayListWritableAggregator();
		aggr.setConf(immConf);
		
		ArrayListOfDoubleArrayListWritable other = new ArrayListOfDoubleArrayListWritable();
		DoubleArrayListWritable value = new DoubleArrayListWritable();
		value.add(new DoubleWritable(1.0));
		value.add(new DoubleWritable(2.0));
		other.add(value);
		
		aggr.aggregate(other);
		assertEquals(1.0, aggr.getAggregatedValue().get(0).get(0).get(), E);
		assertEquals(2.0, aggr.getAggregatedValue().get(0).get(1).get(), E);
		
		ArrayListOfDoubleArrayListWritable other2 = new ArrayListOfDoubleArrayListWritable();
		DoubleArrayListWritable value2 = new DoubleArrayListWritable();
		value2.add(new DoubleWritable(3.0));
		value2.add(new DoubleWritable(4.0));
		other2.add(value2);
		
		aggr.aggregate(other2);
		assertEquals(3.0, aggr.getAggregatedValue().get(1).get(0).get(), E);
		assertEquals(4.0, aggr.getAggregatedValue().get(1).get(1).get(), E);
		
		ArrayListOfDoubleArrayListWritable other3 = new ArrayListOfDoubleArrayListWritable();
		DoubleArrayListWritable value3 = new DoubleArrayListWritable();
		value3.add(new DoubleWritable(5.0));
		value3.add(new DoubleWritable(6.0));
		other3.add(value3);
		
		ArrayListOfDoubleArrayListWritable other4 = new ArrayListOfDoubleArrayListWritable();
		DoubleArrayListWritable value4 = new DoubleArrayListWritable();
		value4.add(new DoubleWritable(7.0));
		value4.add(new DoubleWritable(8.0));
		other4.add(value4);
		
		aggr.aggregate(other3);
		aggr.aggregate(other4);
		
		assertEquals(2, aggr.getAggregatedValue().size());
		System.out.println(aggr.getAggregatedValue().get(0).get(0).get() + ", " +aggr.getAggregatedValue().get(0).get(1).get());
		System.out.println(aggr.getAggregatedValue().get(1).get(0).get() + ", " +aggr.getAggregatedValue().get(1).get(1).get());
	}

}
