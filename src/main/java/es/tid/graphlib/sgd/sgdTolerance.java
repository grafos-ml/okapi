package es.tid.graphlib.sgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.DefaultEdge;
//import org.apache.giraph.graph.Edge;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import es.tid.graphlib.examples.MessageWrapper;
//import java.util.HashMap;
//import java.util.Map;
import java.lang.Math;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
    name = "Stochastic Gradient Descent (SGD)",
    description = "Minimizes the error in users preferences predictions"
)

public class sgdTolerance extends EdgeListVertex<IntWritable, DoubleArrayListWritable, 
IntWritable, MessageWrapper>{
	/** The convergence tolerance */
	static double INIT = 0.5;
	/** SGD vector size **/
	static int SGD_VECTOR_SIZE = 20;
	/** SGD regularization weight */
	static double LAMBDA = 0.001;
	/** SGD step size - learning rate */
	static double GAMMA = 0.001;
	/** Number of supersteps */
	//static double ITERATIONS = 5;
	/** The Convergence Tolerance */
	static double TOLERANCE = 0.003;
	/** Maximum number of updates */
	static int MAX_UPDATES = -1;
	/** Max rating */
	static double MAX = 1e+100;
	/** Min rating */
	static double MIN = -1e+100;
	
	/** Observed Value - Rating */
	private double observed = 0d;
	/** Predicted Value */
	private double predicted = 0d;
	/** Error */    
	public double err = 0d;
	/** Number of times the vertex got updated */
	private int nupdates = 0;
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item = false;
	/** Value of Vertex */
	DoubleArrayListWritable value = new DoubleArrayListWritable();
	/** Class logger */
	private static final Logger LOG =
			Logger.getLogger(sgdTolerance.class);

	sgdTolerance() {
		for (int i=0; i<SGD_VECTOR_SIZE; i++) {
			value.add(new DoubleWritable(INIT));
		}
		setValue(value);
	}
	public void compute(Iterable<MessageWrapper> messages) {
		/** First Superstep for items */
		if (getSuperstep()==1) {		
			item=true;
			System.out.println("item:" + item);
		}		

		/** Array List with errors for printing in the last superstep */
		//ArrayList<Double> errors = new ArrayList<Double>();
		//HashMap<Integer,Double> errmap = new HashMap<Integer,Double>();
				
		System.out.println("*******  Vertex: "+getId()+", superstep:"+getSuperstep());

		/*** For each message */
		for (MessageWrapper message : messages) {
			/*** Debugging */
			if (LOG.isDebugEnabled()) {
				LOG.debug("Vertex " + getId() + " predicts for item " +
						message.getSourceId().get());
			}
			System.out.println("-I am vertex " + getId() + 
					" and received from " + message.getSourceId().get());

			/** Start receiving message from the second superstep */
			if (getSuperstep()==1) {							
				// Save its rating given from the user
				observed = message.getMessage().get(message.getMessage().size()-1).get();
				IntWritable sourceId = message.getSourceId();
				DefaultEdge<IntWritable, IntWritable> edge = new DefaultEdge<IntWritable, IntWritable>();
				edge.setTargetVertexId(sourceId);
				edge.setValue(new IntWritable((int) observed));
				System.out.println("   Adding edge:"+edge);
				addEdge(edge);
				// Remove the last value from message - it's there for the 1st round
				message.getMessage().remove(message.getMessage().size()-1);				
			}
			/*** Calculate error */
			predicted = dotProduct(getValue(), message.getMessage());
			observed = (double)getEdgeValue(message.getSourceId()).get();
			err = getError(predicted, observed);
			System.out.println("BEFORE: error = " + err);
			
			/** Recompute latent values
			 * user_vector = vertex_vector + 
			 * -GAMMA*((dot_product(vertex_vector, other_vertex_vector) - real_value) 
			 * *other_vertex_vector + 
			 * LAMBDA * vertex_vector) */
			//System.out.println("BEFORE:vertex_vector=" + getValue().get(0).get() + "," + getValue().get(1).get()); 
			setValue(dotAddition(getValue(),
					numMatrixProduct((double) -GAMMA,
					(dotAddition(numMatrixProduct((double) err,message.getMessage()),
							numMatrixProduct((double) LAMBDA, getValue()))))));
			//System.out.println("AFTER:vertex_vector=" + getValue().get(0).get() + "," + getValue().get(1).get()); 
			nupdates++;
			
			predicted = dotProduct(getValue(), message.getMessage());
			err = getError(predicted, observed);
			System.out.println("AFTER: error = " + err);
			if (Math.abs(err) > TOLERANCE && nupdates < MAX_UPDATES){
				/** Create a message and wrap together the source id and the message */
				System.out.print("I am vertex " + getId() + " and sent to " + message.getSourceId());
				MessageWrapper sndMessage = new MessageWrapper();
				sndMessage.setSourceId(getId());
				sndMessage.setMessage(getValue());
				sendMessage(message.getSourceId(), sndMessage);
			}
			/*
			if (getSuperstep() == ITERATIONS-2 && item==false 
					|| getSuperstep() == ITERATIONS-1 && item==false) {
				errmap.put(new Integer(getEdgeValue(message.getSourceId()).get()), err);
				System.out.println("------ Error for item " + entry.getKey() + ": " + entry.getValue() + " -------");
			}*/
		} // End of for each message
		voteToHalt();
	}//EofCompute

	/*** Calculate the error: e=observed-predicted */
	public double getError(double predicted, double observed){
		predicted = Math.min(predicted, MAX);
		predicted = Math.max(predicted, MIN);
		return predicted-observed;
	}

	/*** Calculate the dot product of 2 vectors: vector1*vector2 */
	public double dotProduct(DoubleArrayListWritable ma, DoubleArrayListWritable mb){
		double result = 0d;
		for (int i=0; i<SGD_VECTOR_SIZE; i++){
			result += ma.get(i).get() * mb.get(i).get();
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
