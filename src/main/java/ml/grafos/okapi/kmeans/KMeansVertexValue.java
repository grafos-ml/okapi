package ml.grafos.okapi.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/** 
 * The type of the vertex value in K-means
 * It stores the coordinates of the point
 * and the currently assigned cluster id
 *
 */
public class KMeansVertexValue implements Writable {
	private final DoubleArrayListWritable pointCoordinates;
	private IntWritable clusterId;
	
	public KMeansVertexValue(DoubleArrayListWritable coordinates,
			IntWritable id) {
		this.pointCoordinates = coordinates;
		this.clusterId = id;
	}

	public KMeansVertexValue() {
		this.pointCoordinates = new DoubleArrayListWritable();
		this.clusterId = new IntWritable();
	}

	public DoubleArrayListWritable getPointCoordinates() {
		return this.pointCoordinates;
	}
	
	public IntWritable getClusterId() {
		return this.clusterId;
	}
	
	public void setClusterId(IntWritable id) {
		this.clusterId = id;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		pointCoordinates.readFields(in);
		clusterId.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		pointCoordinates.write(out);
		clusterId.write(out);		
	}

}
