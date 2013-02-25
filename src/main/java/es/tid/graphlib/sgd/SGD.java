package es.tid.graphlib.sgd;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class SGD extends EdgeListVertex<IntWritable, SGD.DoubleArrayListWritable, 
IntWritable, SGD.DoubleArrayListWritable>{
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

	public void compute(Iterable<SGD.DoubleArrayListWritable> messages) {		
		double observed;
		double predicted;
		double e;
		
		if (getSuperstep()==0){
			/** Initialize vertex values (feature space coordinates) to zero */
			setValue(new SGD.DoubleArrayListWritable());
			/*** In the 1st round, the users (even numbers-ids) begin */ 
			if (getId().get()%2 != 0)
				voteToHalt();
		}
		/*
		 * 
		 *   public E getEdgeValue(I targetVertexId) {
    			for (Edge<I, E> edge : getEdges()) {
      				if (edge.getTargetVertexId().equals(targetVertexId)) {
        				return edge.getValue();
      				}
    			}
		 * 
		 */
		Edge<IntWritable, IntWritable> edge;
		/*** For each message */
		for (SGD.DoubleArrayListWritable message : messages) {
			observed = getEdgeValue(new IntWritable((int)message.get(0).get()));
		
			/*** Compute for each neighbor */
			for (edge : getEdges()) {
				/*** Debugging */
				if (LOG.isDebugEnabled()) {
					LOG.debug("Vertex " + getId() + " predicts for item " +
							edge.getTargetVertexId());
				}
				/*** Known value */
				observed = edge.getValue().get();
				/*** Predicted value */
				predicted = dotProduct(getValue(), message);
				/*** Calculate error */
				e = observed - predicted;
			
				/** user_vector = user_vector + 
				 * 2*GAMMA*(real_value - dot_product(user_vector,item_vector>))*item_vector + 
				 * LAMBDA * user_vector */
				setValue(dotAddition(dotAddition(getValue(), 
					numMatrixProduct((float) (2*GAMMA*e), message)),
					numMatrixProduct((float) LAMBDA, getValue())));
			}
		}
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
	public double dotProduct(SGD.DoubleArrayListWritable ma, SGD.DoubleArrayListWritable mb){
		return (ma.get(1).get() * mb.get(1).get() + ma.get(2).get() * mb.get(2).get());
	}
	
	/*** Calculate the dot product of 2 vectors */
	public SGD.DoubleArrayListWritable dotAddition(
			SGD.DoubleArrayListWritable ma, 
			SGD.DoubleArrayListWritable mb){
		SGD.DoubleArrayListWritable result = new SGD.DoubleArrayListWritable();
		result.set(1, new DoubleWritable(ma.get(1).get() + mb.get(1).get()));
		result.set(2, new DoubleWritable(ma.get(2).get() + mb.get(2).get()));
		return result;
	}
	
	/*** Calculate the dot product of 2 vectors */
	public SGD.DoubleArrayListWritable numMatrixProduct(double num, SGD.DoubleArrayListWritable matrix){
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
