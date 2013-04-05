package es.tid.graphlib.als;


import java.util.Map.Entry;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import es.tid.graphlib.utils.DoubleArrayListHashMapWritable;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

import org.jblas.DoubleMatrix;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
		name = "Alternating Least Squares (ALS)",
		description = "Matrix Factorization Algorithm: " +
				"It Minimizes the error in users preferences predictions"
		)

public class Als extends EdgeListVertex<IntWritable, DoubleArrayListHashMapWritable, 
IntWritable, MessageWrapper>{
	/** Vector size **/
	static int VECTOR_SIZE = 2;
	/** Regularization parameter */
	static double LAMBDA = 0.005;
	/** Number of supersteps */
	static double ITERATIONS = 10;
	/** Convergence Tolerance */
	static double TOLERANCE = 0.0003;
	/** Max rating */
	static double MAX = 5;
	/** Min rating */
	static double MIN = 0;
	/** Decimals */
	static int DECIMALS = 4;
	/** Number of updates */
	public int nupdates = 0;
	/** The most recent change in the factor value */
	public double residual = 0d;
	/** Observed Value - Rating */
	private double observed = 0d;
	/** Error */    
	public double err = 0d;
	/** Factor Error: it may be RMSD or L2NORM on initial&final vector  */
	public double err_factor=0d;
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item = false;

	public void compute(Iterable<MessageWrapper> messages) {
		/** Counter of messages received - different from getNumEdges() 
		 * because a neighbor may not send a message */
		int msgCounter = 0;
		/** Flat for checking if delta caching is enabled */
		boolean deltaFlag = getContext().getConfiguration().getBoolean("als.delta", false);

		/** First superstep for users (superstep: 0) & items (superstep: 1) */
		if (getSuperstep() < 2){ 
			initLatentVector();
		}
		/** Set flag for items */ 
		if (getSuperstep() == 1) {		
			item=true;
		}
				System.out.println("*******  Vertex: "+getId()+", superstep:"+getSuperstep()+", item:" + item + 
		", " + getValue().getLatentVector()); 
		 
		msgCounter=0;
		/** For each message */
		for (MessageWrapper message : messages) {
			msgCounter++;
			System.out.println("  [RECEIVE] from " + message.getSourceId().get()
									+ ", " + message.getMessage());
						
			/** First superstep for items 
			 *  --> Add Edges connecting to users
			 *  --> Store the rating given from users
			 */
			if (getSuperstep() == 1) {
				observed = message.getMessage().get(message.getMessage().size()-1).get();
				DefaultEdge<IntWritable, IntWritable> edge = new DefaultEdge<IntWritable, IntWritable>();
				edge.setTargetVertexId(message.getSourceId());
				edge.setValue(new IntWritable((int) observed));
				//System.out.println("   Adding edge:" + edge);
				addEdge(edge);
				// Remove the last value from message - it's there for the 1st round
				message.getMessage().remove(message.getMessage().size()-1);	
			}
			if (deltaFlag) {
				/** Create table with neighbors latent values and ids */
				if (getSuperstep() == 1 || getSuperstep() == 2) {
					getValue().setNeighborValue(message.getSourceId(), message.getMessage());
				}
			
				if (getSuperstep() > 2) {
					updateNeighValues(getValue().getNeighValue(message.getSourceId()), message.getMessage());
				}
			}
			if (!deltaFlag) {
				/*** Calculate error */
				//observed = (double)getEdgeValue(message.getSourceId()).get();
				//err = getError(getValue().getLatentVector(), message.getMessage(), observed);
				/** Change the Vertex Latent Vector based on SGD equation */
				//runAlsAlgorithm(message.getMessage());
				//DoubleArrayListWritable ra = new DoubleArrayListWritable();
				//ra = numMatrixProduct(err,message.getMessage());
				//nupdates++;
				//err = getError(getValue().getLatentVector(), message.getMessage(),observed);
				/* If termination flag is set to RMSD or RMSD aggregator is true */
				/*if (factorFlag.equals("rmsd") || rmsdFlag) {
					rmsdErr+= Math.pow(err, 2);
				}*/
			}
		} // Eof Messages
		if (deltaFlag) {
			if (getSuperstep()>0) {
				int i=0;
				DoubleMatrix mat = new DoubleMatrix(getNumEdges(),VECTOR_SIZE);
				System.out.println("mat is created");
				for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()){
					DoubleMatrix v = new DoubleMatrix(1, VECTOR_SIZE, (double)getEdgeValue(vvertex.getKey()).get());
					mat.putRow(i, v);
					System.out.println("mat.rows: " + mat.rows + ", mat.columns: " + mat.columns);
					i++;
					/*** Calculate error */
					observed = (double)getEdgeValue(vvertex.getKey()).get();
					err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
					System.out.println("BEFORE: error = " + err + " vertex_vector= " + getValue().getLatentVector());
					/** Change the Vertex Latent Vector based on SGD equation */
					//runAlsAlgorithm(vvertex.getValue());
					DoubleMatrix A = mat.mmul(mat.transpose());
					//DoubleMatrix V = mat.mm
					double reg = LAMBDA * getNumEdges();
					nupdates++;
					err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
					System.out.println("AFTER: error = " + err + " vertex_vector= " + getValue().getLatentVector());
					/** If termination flag is set to RMSD or RMSD aggregator is true */
					/*if (factorFlag.equals("rmsd") || rmsdFlag) {
						rmsdErr+= Math.pow(err, 2);
					}*/
				}
			}
		}
		err_factor = TOLERANCE + 1;
		if (getSuperstep()==0 || (err_factor > TOLERANCE && getSuperstep()<ITERATIONS)){
			sendMsgs();
		}
		err_factor = err;
		voteToHalt();
	} // Eof compute()
	
	/*** Return type of current vertex */
	public boolean isItem(){
		return item;
	}
	
	/*** Initialize Vertex Latent Vector */
	public void initLatentVector(){
		DoubleArrayListHashMapWritable value = new DoubleArrayListHashMapWritable();
		DoubleArrayListWritable latentVector = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++) {
			latentVector.add(new DoubleWritable(((double)(getId().get()+i) % 100d)/100d));
		}
		value.setLatentVector(latentVector);
		setValue(value);
		//System.out.println("[INIT] value: " + value.getLatentVector());
	}
	
	/*** Return amount of vertex updates */
    public int getUpdates(){
    	return nupdates;
    }

    /*** Update Vertex Latent Vector based on ALS equation */
	public void runAlsAlgorithm(DoubleArrayListWritable vvertex){

		// from paper
		DoubleArrayListWritable la, ra, value = new DoubleArrayListWritable();
		double num = LAMBDA * getNumEdges();
		la = numMatrixProduct(num, getValue().getLatentVector());
		ra = numMatrixProduct(err,vvertex);
		value = dotAddition(getValue().getLatentVector(), dotAddition(la, ra));
		keepXdecimals(value, DECIMALS);
		//System.out.println(" , 4 decimals: " + value);
		getValue().setLatentVector(value);
		
		// from als.cpp
		// XtX = neighbor.vector * neighbor.vector.transpose
		// Xy = neighbor.vector * rating
		DoubleArrayListWritable  Xy, oldVal, newVal, diff = new DoubleArrayListWritable();
		double XtX = dotProduct(vvertex,vvertex);
		Xy = numMatrixProduct(observed,vvertex);
		double reg = LAMBDA * getNumEdges();
		double regXtx = XtX + reg;
		oldVal = getValue().getLatentVector();
		newVal = dotAddition(getValue().getLatentVector(), Xy);
		diff = dotSubtraction(newVal,oldVal);
		double result = 0d;
		for (int i=0; i < diff.size(); i++){
			result += diff.get(i).get();
		}
		result = result / getNumEdges();
	}
	
	/*** Update Neighbor's values */
	public boolean updateNeighValues(DoubleArrayListWritable curVal, DoubleArrayListWritable latestVal){
		boolean updated=false;
		for (int i=0; i<VECTOR_SIZE; i++){
			if (latestVal.get(i) != curVal.get(i)){
				curVal.set(i, latestVal.get(i));
				updated=true;
			}
			else {
				System.out.println("[COMPARE]" + curVal.get(i) + ", " + latestVal.get(i));
			}
		}
		return updated;
	}
	
	/*** Send messages to neighbours */
	public void sendMsgs(){
		/** Send to all neighbors a message*/
		for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			/** Create a message and wrap together the source id and the message */
			MessageWrapper message = new MessageWrapper();
			message.setSourceId(getId());
			message.setMessage(getValue().getLatentVector());
			// 1st superstep, users send rating to items
			if (getSuperstep()==0) {
				DoubleArrayListWritable x = new DoubleArrayListWritable(getValue().getLatentVector());
				x.add(new DoubleWritable(edge.getValue().get()));
				message.setMessage(x);
			} 
			sendMessage(edge.getTargetVertexId(), message);
			System.out.println("  [SEND] to " + edge.getTargetVertexId() + 
					" (rating: " + edge.getValue() + ")" +
					" [" + getValue().getLatentVector() + "]");
		} // End of for each edge
	}
	
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
		for (int i=0; i<VECTOR_SIZE; i++){
			result += (ma.get(i).get() * mb.get(i).get());
		}
		return result;
	}
	
	/*** Calculate the dot addition of 2 vectors: vector1+vector2 */
	public DoubleArrayListWritable dotAddition(
			DoubleArrayListWritable ma, 
			DoubleArrayListWritable mb){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++){
			result.add(new DoubleWritable(ma.get(i).get() + mb.get(i).get()));
		}
		return result;
	}

	/*** Calculate the dot addition of 2 vectors: vector1+vector2 */
	public DoubleArrayListWritable dotSubtraction(
			DoubleArrayListWritable ma, 
			DoubleArrayListWritable mb){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++){
			result.add(new DoubleWritable(ma.get(i).get() - mb.get(i).get()));
		}
		return result;
	}
	/*** Calculate the product num*matirx */
	public DoubleArrayListWritable numMatrixProduct(double num, DoubleArrayListWritable matrix){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++){
			result.add(new DoubleWritable(num * matrix.get(i).get()));
		}
		return result;
	}
	
	/*** Decimal Precision of latent vector values */
	public void keepXdecimals(DoubleArrayListWritable value, int x){
		double num=1;
		for (int i=0; i<x; i++){
			num*=10;
		}
		for (int i=0; i<value.size(); i++){
			value.set(i, new DoubleWritable((double)(Math.round(value.get(i).get() * num) / num)));
		}
	}
}
