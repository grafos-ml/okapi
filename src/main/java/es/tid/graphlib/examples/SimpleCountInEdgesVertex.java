package es.tid.graphlib.examples;

import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;


/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Count input edges",
    description = "Counts input edges for each vertex"
)
  
public class SimpleCountInEdgesVertex extends
  Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>{
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(SimpleCountInEdgesVertex.class);
	
	public void compute(Iterable<DoubleWritable> messages) {
		/** Initialize vertex value to zero */
		if (getSuperstep() == 0) {
			setValue(new DoubleWritable(0d));
		}
		/** Initialize counter */
		double count = 0d;	
		/** Count messages */
		for (@SuppressWarnings("unused") DoubleWritable message : messages) {
			count+=1d;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex " + getId() + " got a message. New total = " + count);
		}
		/** Save count into vertex value */
		setValue(new DoubleWritable(count));
      
		/** Send to all neighbors a message*/
		for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " sent a message to " +
						edge.getTargetVertexId());
			}
			if (getSuperstep()<2){
				sendMessage(edge.getTargetVertexId(), new DoubleWritable(1d));
			}
		}
		voteToHalt();
	}
}
