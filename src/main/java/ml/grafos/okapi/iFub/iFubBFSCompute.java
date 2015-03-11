/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
/*
 * This class is a version of BFS used in iFub.
 * There is a cache for the sourceID and the current BFS step=distance from source
 * The method propagateDistance update the aggregators for the iFub Algorithm
 */
public class iFubBFSCompute extends BasicComputation<LongWritable, iFubVertexValue, NullWritable, IntWritable>{
	private long Source_ID;
	private long bfsStep;
	
	private boolean isSource(
			Vertex<LongWritable, iFubVertexValue, NullWritable> vertex) {
		return vertex.getId().get() == Source_ID;
	}
	@Override
	public void preSuperstep() {
		// TODO Auto-generated method stub
		super.preSuperstep();
		this.bfsStep = ((LongWritable)getAggregatedValue(iFubMasterCompute.BFS_STEP)).get();
		if(bfsStep==0)
			this.Source_ID = ((LongWritable)getAggregatedValue(iFubMasterCompute.SOURCE_ID)).get();
	}
	@Override
	public void compute(
			Vertex<LongWritable, iFubVertexValue, NullWritable> vertex,
			Iterable<IntWritable> messages) throws IOException {

		if (bfsStep==0) {
			if(vertex.getValue()==null)
			{
				vertex.setValue(new iFubVertexValue());
			}

			vertex.getValue().setDistance(Long.MAX_VALUE);
			if (isSource(vertex)) {
				vertex.getValue().setSource(true);
				propagateDistance(vertex);
			}
			
		}
		else
		{
			if (messages.iterator().hasNext()
					&& vertex.getValue().getDistance() == Long.MAX_VALUE) {
				propagateDistance(vertex);
			}
		}
	}

	private void propagateDistance(
			Vertex<LongWritable, iFubVertexValue, NullWritable> vertex)
			throws IOException {
		aggregate(iFubMasterCompute.NUM_ACTIVE_VERTEX, new LongWritable(1));

		aggregate(iFubMasterCompute.LOWER_B,new LongWritable(bfsStep));
		vertex.getValue().setDistance(bfsStep);
		sendMessageToAllEdges(vertex, new IntWritable(0));
		for(Edge<LongWritable,NullWritable> arrete : vertex.getEdges())
		{
			sendMessage(arrete.getTargetVertexId(), new IntWritable(1));
		}
	}
	

}
