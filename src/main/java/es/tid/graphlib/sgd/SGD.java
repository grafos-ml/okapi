package es.tid.graphlib.sgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import es.tid.graphlib.examples.MessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions"
)

public class SGD extends EdgeListVertex<IntWritable, DoubleArrayListWritable, 
IntWritable, MessageWrapper>{
	/** The convergence tolerance */
	//static double INIT=0.5;
	/** Regularization parameter */
	static double LAMBDA= 0.001;
	/** Learning rate */
	static double GAMMA=0.001;
	/** Number of supersteps */
	static double ITERATIONS=5;
	    
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(SGD.class);

	public void compute(Iterable<MessageWrapper> messages) {		
		double observed;
		double predicted;
		double e;
		
		if (getSuperstep()==0){
			/** Initialize vertex values (feature space coordinates) to zero */
			setValue(new DoubleArrayListWritable());
			/*** In the 1st round, the users (even numbers-ids) begin */ 
			if (getId().get()%2 != 0)
				voteToHalt();
		}
		
		/*** For each message */
		for (MessageWrapper message : messages) {
			/*** Debugging */
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " predicts for item " +
						message.getSourceId().get());
			}
			/*** Known value */
			observed = (double)getEdgeValue(message.getSourceId()).get();			
			/*** Predicted value */
			predicted = dotProduct(getValue(), message.getMessage());
			/*** Calculate error */
			e = observed - predicted;
			/** user_vector = user_vector + 
			 * 2*GAMMA*(real_value - dot_product(user_vector,item_vector>))*item_vector + 
			 * LAMBDA * user_vector */
			setValue(dotAddition(dotAddition(getValue(), 
					numMatrixProduct((float) (2*GAMMA*e), message.getMessage())),
					numMatrixProduct((float) LAMBDA, getValue())));
		}
		/** Send to all neighbors a message*/
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " sent a message to " +
						edge.getTargetVertexId());
			}
			/** Create a message and wrap together the source id and the message */
			MessageWrapper message = new MessageWrapper();
			message.setSourceId(getId());
			message.setMessage(getValue());
			if (getSuperstep()<ITERATIONS){
				sendMessage(edge.getTargetVertexId(), message);
			}
		}
		voteToHalt();
	}//EofCompute

	/*** Calculate the dot product of 2 vectors vector1*vector2 */
	public double dotProduct(DoubleArrayListWritable ma, DoubleArrayListWritable mb){
		return (ma.get(0).get() * mb.get(0).get() 
				+ ma.get(1).get() * mb.get(1).get());
	}
	
	/*** Calculate the dot addition of 2 vectors vector1+vector2 */
	public DoubleArrayListWritable dotAddition(
			DoubleArrayListWritable ma, 
			DoubleArrayListWritable mb){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		result.set(0, 
				new DoubleWritable(ma.get(0).get()
						+ mb.get(0).get()));
		result.set(1, 
				new DoubleWritable(ma.get(1).get() 
						+ mb.get(1).get()));
		return result;
	}
	
	/*** Calculate the product num*matirx */
	public DoubleArrayListWritable numMatrixProduct(double num, DoubleArrayListWritable matrix){
		matrix.set(0, new DoubleWritable(num * matrix.get(0).get()));
		matrix.set(1, new DoubleWritable(num * matrix.get(1).get()));
		return matrix;
	}
}
