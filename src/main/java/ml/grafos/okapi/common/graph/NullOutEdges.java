package ml.grafos.okapi.common.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class NullOutEdges implements OutEdges<LongWritable, NullWritable> {

	@Override
	public Iterator<Edge<LongWritable, NullWritable>> iterator() {
		return null;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize(Iterable<Edge<LongWritable, NullWritable>> edges) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize(int capacity) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void add(Edge<LongWritable, NullWritable> edge) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(LongWritable targetVertexId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
