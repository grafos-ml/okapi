/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
/*
 * This class is used to select a source before running a BFS
 * the current layer is cached before the step
 * a vertex is select if it match the currentStep and if it wasn't a source yet
 */
public class iFubSelectSource extends BasicComputation<LongWritable, iFubVertexValue, NullWritable, IntWritable>{

	long currentLayer;
	@Override
	public void preSuperstep() {
		// TODO Auto-generated method stub
		super.preSuperstep();
		currentLayer = ((LongWritable)getAggregatedValue(iFubMasterCompute.LAYER)).get();
	}
	
	@Override
	public void compute(
			Vertex<LongWritable, iFubVertexValue, NullWritable> vertex,
			Iterable<IntWritable> arg1) throws IOException {
		//le sommet est de la couche demandé et il n'as pas été la source
		if(vertex.getValue().getLayer()==currentLayer&&!vertex.getValue().wasSource())
		{
			aggregate(iFubMasterCompute.SOURCE_ID,vertex.getId());
		}
	}

}
