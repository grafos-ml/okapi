package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestKMeansVertexValue {
	private static double E = 0.0001f;

	@Test
	public void testSerialize() {
		DoubleArrayListWritable coordinates = new DoubleArrayListWritable();
		IntWritable clusterId = new IntWritable(5);
		coordinates.add(new DoubleWritable(1.0));
		coordinates.add(new DoubleWritable(2.0));
		coordinates.add(new DoubleWritable(3.0));
		
		// Serialize from
		KMeansVertexValue from = new KMeansVertexValue(coordinates, clusterId);
		byte[] data = WritableUtils.writeToByteArray(from, from);
		
		// De-serialize to
		KMeansVertexValue to1 = new KMeansVertexValue();
		KMeansVertexValue to2 = new KMeansVertexValue();
		
		WritableUtils.readFieldsFromByteArray(data, to1, to2);
		
		// all coordinates should be equal
		assertEquals(from.getPointCoordinates().get(0).get(), to1.getPointCoordinates().get(0).get(), E);
		assertEquals(from.getPointCoordinates().get(1).get(), to1.getPointCoordinates().get(1).get(), E);
		assertEquals(from.getPointCoordinates().get(2).get(), to1.getPointCoordinates().get(2).get(), E);
		
		assertEquals(from.getPointCoordinates().get(0).get(), to2.getPointCoordinates().get(0).get(), E);
		assertEquals(from.getPointCoordinates().get(1).get(), to2.getPointCoordinates().get(1).get(), E);
		assertEquals(from.getPointCoordinates().get(2).get(), to2.getPointCoordinates().get(2).get(), E);
		
		// cluster ids should be equal
		assertEquals(from.getClusterId().get(), to1.getClusterId().get());
		assertEquals(from.getClusterId().get(), to1.getClusterId().get());
	}

}
