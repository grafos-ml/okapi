package es.tid.graphlib.sgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

import java.util.HashMap;
import java.util.Map;
import java.lang.Math;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions"
)

public class SgdAvgVerErr extends Vertex<IntWritable, DoubleArrayListWritable, 
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
	static double ITERATIONS=10;
	/** Tolerance */
	static double TOLERANCE = 0.3;
	/** Max rating */
	static double MAX=5;
	/** Min rating */
	static double MIN=0;
	/** Error */    
	public double err;
	/** Observed Value - Rating */
	private double observed;
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item=false;
	/** Average Vertex Error */
	private double avgErr=0d;
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(SgdAvgVerErr.class);

	public void compute(Iterable<MessageWrapper> messages) {
		/** Value of Vertex */
		DoubleArrayListWritable value = new DoubleArrayListWritable();
		/** Array List with errors for printing in the last superstep */
		HashMap<Integer,Double> errmap = new HashMap<Integer,Double>();
		/** Counter of messages received */
		int msgCounter = 0;
		
		/* If it's the first round for users (0) or 
		 * or if it's the first round for items (1)
		 */
		if (getSuperstep() < 2){ 
			for (int i=0; i<SGD_VECTOR_SIZE; i++) {
				value.add(new DoubleWritable(INIT));
			}
			setValue(value);
		}
		/** First Superstep for items */
		if (getSuperstep() == 1) {		
			item=true;
		}
		System.out.println("*******  Vertex: "+getId()+", superstep:"+getSuperstep()+", item:" + item + 
				", [" + getValue().get(0).get() + "," + getValue().get(1).get() + "]"); 
		
		avgErr=0d;
		/*** For each message */
		for (MessageWrapper message : messages) {
			/*** Debugging */
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " predicts for item " +
						message.getSourceId().get());
			}
			msgCounter++;
			System.out.println("  [RECEIVE] from " + message.getSourceId().get()
					+ " [" + message.getMessage().get(0) + "," + message.getMessage().get(1) + "]");
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
			observed = (double)getEdgeValue(message.getSourceId()).get();
			err = getError(getValue(), message.getMessage(), observed);
			/** user_vector = vertex_vector + 
			 * 2*GAMMA*(real_value - 
			 * dot_product(vertex_vector,other_vertex_vector))*other_vertex_vector + 
			 * LAMBDA * vertex_vector */
			System.out.println("BEFORE: error = " + err);
			System.out.println("BEFORE:vertex_vector= " + getValue().get(0).get() + "," + getValue().get(1).get()); 
			setValue(dotAddition(getValue(),
					numMatrixProduct((double) -GAMMA,
					(dotAddition(numMatrixProduct((double) err,message.getMessage()),
							numMatrixProduct((double) LAMBDA, getValue())))))); 
			err = getError(getValue(), message.getMessage(),observed);
			System.out.println("AFTER: vertex_vector = " + getValue().get(0).get() + "," + getValue().get(1).get());
			System.out.println("AFTER: error = " + err);
			avgErr+=Math.abs(err);
			System.out.println("AvgErr: " + avgErr);
			/** For printing purposes */
			if (getSuperstep() == ITERATIONS-2 && item==false 
					|| getSuperstep() == ITERATIONS-1 && item==false) {
				errmap.put(new Integer(getEdgeValue(message.getSourceId()).get()), err);
			}
		} // End of for each message
		// Average new error from all computations
		avgErr=avgErr/msgCounter;
		System.out.println("Final AvgErr: " + avgErr);
		if (getSuperstep()==0 || (avgErr > TOLERANCE && getSuperstep()<ITERATIONS)){
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
				
				if (getSuperstep()==0) {
					message.getMessage().add(
							new DoubleWritable(edge.getValue().get()));
				}
				sendMessage(edge.getTargetVertexId(), message);
				System.out.println("  [SEND] to " + edge.getTargetVertexId() + 
						" (rating: " + edge.getValue() + ")" +
						" [" + getValue().get(0) + "," + getValue().get(1) + "]");
			} // End of for each edge				
			System.out.println();
			if (getSuperstep() == ITERATIONS-1 && item==false 
					|| getSuperstep() == ITERATIONS && item==false) {
				for (Map.Entry<Integer, Double> entry : errmap.entrySet()) {
				    System.out.println("------ Error for item " + entry.getKey() + ": " + entry.getValue() + " -------");
				}
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
	public double  getError(){
		return err;
	}
	/*** Calculate the dot product of 2 vectors: vector1*vector2 */
	public double dotProduct(DoubleArrayListWritable ma, DoubleArrayListWritable mb){
		double result = 0d;
		for (int i=0; i<SGD_VECTOR_SIZE; i++){
			result += (ma.get(i).get() * mb.get(i).get());
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