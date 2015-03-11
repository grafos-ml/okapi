package ml.grafos.okapi.iFub;

import java.io.IOException;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class ToUndirected extends BasicComputation<LongWritable, NullWritable, NullWritable, LongWritable>{

	@Override
	public void compute(Vertex<LongWritable, NullWritable, NullWritable> vertex, Iterable<LongWritable> inbox) throws IOException {
		// TODO Auto-generated method stub
		if(getSuperstep()==0)
		{
			sendMessageToAllEdges(vertex, vertex.getId());
		}
		else if(getSuperstep()==1)
		{
			for(LongWritable sourceId : inbox)
			{
				if(vertex.getEdgeValue(sourceId)==null)
				{
					vertex.addEdge(EdgeFactory.create(sourceId));
				}
			}
		}
		vertex.voteToHalt();
		
	}

}
