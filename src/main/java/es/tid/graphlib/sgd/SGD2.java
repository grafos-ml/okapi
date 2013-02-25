package es.tid.graphlib.sgd;

import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions"
)

public class SGD2 extends EdgeListVertex<IntWritable, SGD2.DoubleArrayListWritable, 
IntWritable, SGD2.DoubleArrayListWritable>{
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
			Logger.getLogger(SGD2.class);

	public void compute(Iterable<SGD2.DoubleArrayListWritable> messages) {		
		double observed;
		double predicted;
		double e;
		
		if (getSuperstep()==0){
			/** Initialize vertex values (feature space coordinates) to zero */
			setValue(new SGD2.DoubleArrayListWritable());
			/*** In the 1st round, the users (even numbers-ids) begin */ 
			if (getId().get()%2 != 0)
				voteToHalt();
		}
		
		/*** Compute for each neighbor */
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			/*** Debugging */
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " predicts for item " +
						edge.getTargetVertexId());
			}
			SGD2.DoubleArrayListWritable msg = new SGD2.DoubleArrayListWritable();
			/*** Find the message from this neighbor */
			for (SGD2.DoubleArrayListWritable message : messages) {
				if (message.get(0).get() == edge.getTargetVertexId().get()){
					msg = message;
				}
			}
			/*** Known value */
			observed = edge.getValue().get();
			/*** Predicted value */
			predicted = dotProduct(getValue(), msg);
			/*** Calculate error */
			e = observed - predicted;
			
			/** user_vector = user_vector + 
			 * 2*GAMMA*(real_value - dot_product(user_vector,item_vector>))*item_vector + 
			 * LAMBDA * user_vector */
			setValue(dotAddition(dotAddition(getValue(), 
					numMatrixProduct((float) (2*GAMMA*e), msg)),
					numMatrixProduct((float) LAMBDA, getValue())));
		} //Eof for each neighbor
		
		/** Send to all neighbors a message*/
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " sent a message to " +
						edge.getTargetVertexId());
			}
			if (getSuperstep()<ITERATIONS){
				sendMessage(edge.getTargetVertexId(), getValue());
			}
		}
		voteToHalt();
	}//EofCompute

	/*** Calculate the dot product of 2 vectors */
	public double dotProduct(SGD2.DoubleArrayListWritable ma, SGD2.DoubleArrayListWritable mb){
		return (ma.get(1).get() * mb.get(1).get() + ma.get(2).get() * mb.get(2).get());
	}
	
	/*** Calculate the dot product of 2 vectors */
	public SGD2.DoubleArrayListWritable dotAddition(
			SGD2.DoubleArrayListWritable ma, 
			SGD2.DoubleArrayListWritable mb){
		SGD2.DoubleArrayListWritable result = new SGD2.DoubleArrayListWritable();
		result.set(1, new DoubleWritable(ma.get(1).get() + mb.get(1).get()));
		result.set(2, new DoubleWritable(ma.get(2).get() + mb.get(2).get()));
		return result;
	}
	
	/*** Calculate the dot product of 2 vectors */
	public SGD2.DoubleArrayListWritable numMatrixProduct(double num, SGD2.DoubleArrayListWritable matrix){
		matrix.set(1, new DoubleWritable(num * matrix.get(1).get()));
		matrix.set(2, new DoubleWritable(num * matrix.get(2).get()));
		return matrix;
	}

  	public static class DoubleArrayListWritable
  	extends ArrayListWritable<DoubleWritable>{
  		/** Default constructor for reflection */
		public DoubleArrayListWritable() {
			super();
		}
		@Override
		@SuppressWarnings("unchecked")
		public void setClass() {
			setClass(DoubleWritable.class);
		}
	}
}
