package es.tid.graphlib.sgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
//import java.util.HashMap;
//import java.util.Map;
//import java.lang.Math;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions"
)

public class SgdTolerance extends Vertex<IntWritable, DoubleArrayListWritable, 
IntWritable, MessageWrapper>{
	/** The convergence tolerance */
	static double INIT=0.5;
	/** SGD vector size **/
	static int SGD_VECTOR_SIZE=2;
	/** Regularization parameter */
	static double LAMBDA= 0.005;
	/** Learning rate */
	static double GAMMA=0.01;
	/** Number of supersteps */
	//static double ITERATIONS=100;
	/** The Convergence Tolerance */
	static double TOLERANCE = 0.003;
	/** Maximum number of updates */
	static int MAX_UPDATES = 100;
	/** Max rating */
	static double MAX=1e+100;
	/** Min rating */
	static double MIN=-1e+100;
	/** Error */    
	public Double err = 0d;
	/** Observed Value - Rating */
	private double observed = 0d;
	/** Number of times the vertex got updated */
	private int nupdates = 0;
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item=false;
	/** Class logger */
	private static final Logger LOG = Logger.getLogger(SgdTolerance.class);
	
	public void compute(Iterable<MessageWrapper> messages) {
		/** Value of Vertex */
		DoubleArrayListWritable value = new DoubleArrayListWritable();

		/* If it's the first round for users (0) or 
		 * or if it's the first round for items (1)
		 */
		if (getSuperstep()< 2){ 
			for (int i=0; i<SGD_VECTOR_SIZE; i++) {
				value.add(new DoubleWritable(INIT));
			}
			setValue(value);
		}
		
		/** First Superstep for items */
		if (getSuperstep()==1) {		
			item=true;
		}
		System.out.println("*******  Vertex: "+getId()+", superstep:"+getSuperstep()+", item:" + item + 
				", [" + getValue().get(0).get() + "," + getValue().get(1).get() + "]"); 

		/*** For each message */
		for (MessageWrapper message : messages) {
			/*** Debugging */
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " predicts for item " +
						message.getSourceId().get());
			}
			System.out.println("[RECEIVE] from " + message.getSourceId().get());
			//System.out.println("BEFORE:vertex_vector=" + getValue().get(0).get() + "," + getValue().get(1).get()); 
			
			DefaultEdge<IntWritable, IntWritable> edge = new DefaultEdge<IntWritable, IntWritable>();
			
			/** Start receiving message from the second superstep - items*/
			if (getSuperstep()==1) {							
				// Save its rating given from the user
				observed = message.getMessage().get(message.getMessage().size()-1).get();
				System.out.println("observed: " + observed);
				IntWritable sourceId = message.getSourceId();
				edge.setTargetVertexId(sourceId);
				edge.setValue(new IntWritable((int) observed));
				System.out.println("   Adding edge:" + edge);
				addEdge(edge);
				// Remove the last value from message - it's there for the 1st round
				message.getMessage().remove(message.getMessage().size()-1);				
			}
			/*** Calculate error */
			observed = (double) getEdgeValue(message.getSourceId()).get();
			err = getError(getValue(), message.getMessage(), observed);
			if (err.isNaN()){
				System.out.println("[ERROR] Numeric errors.. Try to tune step size and regularization (lambda and gamma)");
			}
			System.out.println("BEFORE: error = " + err);
			/** user_vector = vertex_vector + 
			 * 2*GAMMA*(real_value - 
			 * dot_product(vertex_vector,other_vertex_vector))*other_vertex_vector + 
			 * LAMBDA * vertex_vector */
			System.out.println("BEFORE:vertex_vector= " + getValue().get(0).get() + "," + getValue().get(1).get()); 
			setValue(dotAddition(getValue(),
					numMatrixProduct((double) -GAMMA,
					(dotAddition(numMatrixProduct((double) err,message.getMessage()),
							numMatrixProduct((double) LAMBDA, getValue()))))));
			nupdates++;
			err = getError(getValue(), message.getMessage(), observed);
			System.out.println("AFTER:vertex_vector = " + getValue().get(0).get() + "," + getValue().get(1).get()); 
			System.out.println("AFTER: error = " + err);
			
			if (Math.abs(err) > TOLERANCE && nupdates < MAX_UPDATES){
				System.out.println("err = " + Math.abs(err) + ", nupdates = " + nupdates);
				/** Create a message and wrap together the source id and the message */
				System.out.println("[SEND] to " + message.getSourceId());
				MessageWrapper sndMessage = new MessageWrapper();
				sndMessage.setSourceId(getId());
				sndMessage.setMessage(getValue());
				sendMessage(message.getSourceId(), sndMessage);
			}
		} // End of for each message
		
		if (getSuperstep()==0){
			for (Edge<IntWritable, IntWritable> edge : getEdges()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Vertex " + getId() + " sent a message to " +
							edge.getTargetVertexId());
				}
				/** Create a message and wrap together the source id and the message */
				MessageWrapper message = new MessageWrapper();
				message.setSourceId(getId());
				message.setMessage(getValue());
				message.getMessage().add(new DoubleWritable(edge.getValue().get()));		
				sendMessage(edge.getTargetVertexId(), message);
				System.out.println("[SEND] to " + edge.getTargetVertexId() + " (rating: " + edge.getValue() + ")");
			}
		}
		voteToHalt();
	}//EofCompute

	/*** Calculate the error: e=observed-predicted */
	public double getError(DoubleArrayListWritable ma, DoubleArrayListWritable mb, double observed){
		/*** Predicted value */
		double predicted = dotProduct(ma,mb);
		predicted = Math.min(predicted, MAX);
		predicted = Math.max(predicted, MIN);
		return predicted-observed;
	}

	/*** Calculate the dot product of 2 vectors: vector1*vector2 */
	public double dotProduct(DoubleArrayListWritable ma, DoubleArrayListWritable mb){
		double result = 0d;
		for (int i=0; i<SGD_VECTOR_SIZE; i++){
			result += (ma.get(i).get() * mb.get(i).get());
			//System.out.println("dotProduct-result:" + result);
		}
		return result;
	}
	
	/*** Calculate the dot addition of 2 vectors: vector1+vector2 */
	public DoubleArrayListWritable dotAddition(
			DoubleArrayListWritable ma, 
			DoubleArrayListWritable mb){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<SGD_VECTOR_SIZE; i++){
			result.add(new DoubleWritable(ma.get(i).get() + mb.get(i).get()));
		}
		return result;
	}
	
	/*** Calculate the product num*matirx */
	public DoubleArrayListWritable numMatrixProduct(double num, DoubleArrayListWritable matrix){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<SGD_VECTOR_SIZE; i++){
			result.add(new DoubleWritable(num * matrix.get(i).get()));
		}
		return result;
	}
}
