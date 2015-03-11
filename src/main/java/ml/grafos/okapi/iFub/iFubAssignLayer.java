/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;
/*
 * This class is used to assign the layer of every vertex and reset the source flag
 */
import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class iFubAssignLayer extends BasicComputation<LongWritable, iFubVertexValue, NullWritable, IntWritable> {

	
	@Override
	public void compute(Vertex<LongWritable, iFubVertexValue, NullWritable> vertex, Iterable<IntWritable> arg1) throws IOException {
		vertex.getValue().setLayer(vertex.getValue().getDistance());
		vertex.getValue().setSource(false);
	}

}
