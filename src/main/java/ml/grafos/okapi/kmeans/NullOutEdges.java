package ml.grafos.okapi.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.NullWritable;

public class NullOutEdges implements OutEdges<NullWritable, NullWritable> {

	@Override
	public Iterator<Edge<NullWritable, NullWritable>> iterator() {
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
	public void initialize(Iterable<Edge<NullWritable, NullWritable>> edges) {
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
	public void add(Edge<NullWritable, NullWritable> edge) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void remove(NullWritable targetVertexId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
