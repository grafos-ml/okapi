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
import org.apache.log4j.Logger;
/*
 * This class simply aggregate the degrees with a maxLong aggregator
 * and aggregate the id of vertices with maximum degree.
 */
public class MaxDegreeComputation extends BasicComputation<LongWritable, iFubVertexValue, NullWritable, IntWritable>{
private Logger LOG = Logger.getLogger(MaxDegreeComputation.class);
	int maxDegree=1;
	boolean sourceFound = false;
	@Override
		public void preSuperstep() {
			super.preSuperstep();
			maxDegree=(int)((LongWritable)getAggregatedValue(iFubMasterCompute.MAX_DEGREE)).get();
		}
	
	@Override
	public void compute(
			Vertex<LongWritable, iFubVertexValue, NullWritable> vertex,
			Iterable<IntWritable> messages) throws IOException {
		if(getSuperstep()==0)
			aggregate(iFubMasterCompute.MAX_DEGREE, new LongWritable(vertex.getNumEdges()));
		else
		{
			if(vertex.getNumEdges()==maxDegree&&!sourceFound)
			{
				sourceFound=true;
				LOG.info("Source FOUND ! "+vertex.getId());
				aggregate(iFubMasterCompute.SOURCE_ID, new LongWritable(vertex.getId().get()));
			}
		}
	}

}
