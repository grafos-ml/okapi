package es.tid.graphlib.sgd;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.vertex.EdgeListVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import es.tid.graphlib.examples.SimpleMasterComputeVertex;
import es.tid.graphlib.utils.DoubleArrayListHashMapWritable;
import es.tid.graphlib.utils.DoubleArrayListWritable;
import es.tid.graphlib.utils.MessageWrapper;

import java.lang.Math;
import java.util.Map.Entry;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
		name = "Stochastic Gradient Descent (SGD)",
		description = "Minimizes the error in users preferences predictions"
		)

public class Sgd extends EdgeListVertex<IntWritable, DoubleArrayListHashMapWritable, 
IntWritable, MessageWrapper>{
	/** Vector Size **/
	static int VECTOR_SIZE = 2;
	/** Regularization parameter */
	static double LAMBDA = 0.005;
	/** Learning rate */
	static double GAMMA = 0.01;
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
	/** Observed Value - Rating */
	private double observed = 0d;
	/** Error */    
	public double err = 0d;
	/** RMSE Error */
	private double rmseErr = 0d;
	/** Factor Error: it may be RMSD or L2NORM on initial&final vector  */
	public double err_factor = 0d;
	/** Initial vector value to be used for the L2Norm case */
	DoubleArrayListWritable initialValue = new DoubleArrayListWritable();
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item = false;
	/** Aggregator to get values from the workers to the master */
	public static final String RMSD_AGG = "rmse.aggregator";
	
	public void compute(Iterable<MessageWrapper> messages) {
		/** Counter of messages received - different from getNumEdges() 
		 * because a neighbor may not send a message */
		int msgCounter = 0;
		/** Flag for checking if parameter for RMSE aggregator received */
		boolean rmseFlag = getContext().getConfiguration().getBoolean("sgd.aggregate", false);
		/** Flag for checking which termination factor to use: basic, rmse, l2norm */
		String factorFlag = getContext().getConfiguration().get("sgd.factor", "basic");
		/** Flat for checking if delta caching is enabled */
		boolean deltaFlag = getContext().getConfiguration().getBoolean("sgd.delta", false);

		/** First superstep for users (superstep: 0) & items (superstep: 1) */
		if (getSuperstep() < 2){ 
			initLatentVector();
		}
		/** Set flag for items */ 
		if (getSuperstep() == 1) {		
			item=true;
		}
		
/*		System.out.println("*******  Vertex: "+getId()+", superstep:"+getSuperstep()+", item:" + item + 
				", " + getValue().getLatentVector()); 
*/		
		rmseErr=0d;
		boolean neighUpdated = false;
		/*** For each message */
		for (MessageWrapper message : messages) {
			msgCounter++;
/*			System.out.println("  [RECEIVE] from " + message.getSourceId().get()
						+ ", " + message.getMessage());
*/			/** First superstep for items 
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
					if (updateNeighValues(getValue().getNeighValue(message.getSourceId()), message.getMessage())) {
						neighUpdated=true;
					}
				}
			}
			if (!deltaFlag) {
				/*** Calculate error */
				observed = (double)getEdgeValue(message.getSourceId()).get();
				err = getError(getValue().getLatentVector(), message.getMessage(), observed);
				/** Change the Vertex Latent Vector based on SGD equation */
				runSgdAlgorithm(message.getMessage());
				err = getError(getValue().getLatentVector(), message.getMessage(),observed);
				/* If termination flag is set to RMSD or RMSD aggregator is true */
				if (factorFlag.equals("rmse") || rmseFlag) {
					rmseErr+= Math.pow(err, 2);
				}
			}
		} // Eof Messages
		if (deltaFlag && getSuperstep()>0) {
			for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()){
				/*** Calculate error */
				observed = (double)getEdgeValue(vvertex.getKey()).get();
				err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
				/** If at least one neighbour has changed its latent vector
				 *  then calculation of vertex can not be avoided */
				if (neighUpdated) {
					/** Change the Vertex Latent Vector based on SGD equation */
					runSgdAlgorithm(vvertex.getValue());
					err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
					/** If termination flag is set to RMSE or RMSE aggregator is true */
					if (factorFlag.equals("rmse") || rmseFlag) {
						rmseErr+= Math.pow(err, 2);
					}
				}
			}
		}
		// If basic factor specified
		if (factorFlag.equals("basic")){
			err_factor=TOLERANCE+1;
		}
		// If RMSE aggregator flag is true
		if (rmseFlag){
			this.aggregate(RMSD_AGG, new DoubleWritable(rmseErr));
		}
		if (factorFlag.equals("rmse")){
			err_factor = getRMSE(msgCounter);
			System.out.println("myRMSD: " + err_factor);
		}
		// If termination factor is set to L2NOrm
		if (factorFlag.equals("l2norm")){
			err_factor = getL2Norm(initialValue, getValue().getLatentVector());
			//System.out.println("NormVector: sqrt((initial[0]-final[0])^2 + (initial[1]-final[1])^2): " 
			//		+ err_factor);
		}
		if (getSuperstep()==0 || (err_factor > TOLERANCE && getSuperstep()<ITERATIONS)){
			sendMsgs();
		}
		// err_factor is used in the OutputFormat file. --> To print the error
		if (factorFlag.equals("basic")){
			err_factor=err;
		}
		voteToHalt();
	}//EofCompute

	/** Return type of current vertex */
	public boolean isItem(){
		return item;
	}
	
	/*** Initialize Vertex Latent Vector */
	public void initLatentVector(){
		DoubleArrayListHashMapWritable value = new DoubleArrayListHashMapWritable();
		for (int i=0; i<VECTOR_SIZE; i++) {
			value.setLatentVector(i,new DoubleWritable(((double)(getId().get()+i) % 100d)/100d));
		}
		setValue(value);
		//System.out.println("[INIT] value: " + value.getLatentVector());
		/** For L2Norm */
		initialValue = getValue().getLatentVector();
	}
	
	/*** Modify Vertex Latent Vector based on SGD equation */
	public void runSgdAlgorithm(DoubleArrayListWritable vvertex){
		/** user_vector = vertex_vector + 
		 * 2*GAMMA*(real_value - 
		 * dot_product(vertex_vector,other_vertex_vector))*other_vertex_vector + 
		 * LAMBDA * vertex_vector */
		DoubleArrayListWritable la, ra, ga, value = new DoubleArrayListWritable();
		la = numMatrixProduct((double) LAMBDA, getValue().getLatentVector());
		ra = numMatrixProduct((double) err,vvertex);
		ga = numMatrixProduct((double) -GAMMA, (dotAddition(ra, la)));
		value = dotAddition(getValue().getLatentVector(), ga);
		//System.out.print("Latent Vector: " + value);
		keepXdecimals(value, DECIMALS);
		//System.out.println(" , 4 decimals: " + value);
		getValue().setLatentVector(value);
		nupdates++;
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
/*			System.out.println("  [SEND] to " + edge.getTargetVertexId() + 
					" (rating: " + edge.getValue() + ")" +
					" [" + getValue().getLatentVector() + "]");
*/		} // End of for each edge
	}

	/*** Calculate the RMSE on the errors calculated by the current vertex */
	public double getRMSE(int msgCounter){
		return Math.sqrt(rmseErr/msgCounter);
	}
	/*** Calculate the RMSD on the errors calculated by the current vertex */
	public double getL2Norm(DoubleArrayListWritable valOld, DoubleArrayListWritable valNew){
		double result=0;
		for (int i=0; i<valOld.size(); i++){
			result += Math.pow((valOld.get(i).get() - valNew.get(i).get()),2);
		}
		System.out.println("L2norm: " + result);
		return Math.sqrt(result);
	}
	/*** Calculate the error: e=observed-predicted */
	public double getError(DoubleArrayListWritable ma, DoubleArrayListWritable mb, double observed){
		/*** Predicted value */
		System.out.println("ma, mb");
		ma.print();
		mb.print();
		
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

	/*** Calculate the product num*matirx */
	public DoubleArrayListWritable numMatrixProduct(double num, DoubleArrayListWritable matrix){
		DoubleArrayListWritable result = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++){
			result.add(new DoubleWritable(num * matrix.get(i).get()));
		}
		return result;
	}
	
	/*** Return amount of vertex updates */
    public int getUpdates(){
    	return nupdates;
    }
	
	/**
	 * MasterCompute used with {@link SimpleMasterComputeVertex}.
	 */
	public static class MasterCompute
	extends DefaultMasterCompute {
		@Override
		public void compute() {
			double numRatings=0;
			double totalRMSD=0;
			if (getSuperstep()>1){
				// In superstep=1 only half edges are created (users to items)
				if (getSuperstep()==2)
					numRatings = getTotalNumEdges();
				else
					numRatings = getTotalNumEdges()/2;

				totalRMSD = Math.sqrt(((DoubleWritable)getAggregatedValue(RMSD_AGG)).get()/numRatings);
					
				System.out.println("Superstep: " + getSuperstep() + ", [Aggregator] Added Values: " + getAggregatedValue(RMSD_AGG)
						+ " / " + numRatings
						+ " = " + ((DoubleWritable)getAggregatedValue(RMSD_AGG)).get()/numRatings
						+ " --> sqrt(): " + totalRMSD);
					
				getAggregatedValue(RMSD_AGG);
				if (totalRMSD < TOLERANCE){
					System.out.println("HALT!");
					haltComputation();
				}
			}
		} // Eof Compute()

		@Override
		public void initialize() throws InstantiationException,
		IllegalAccessException {
				registerAggregator(RMSD_AGG, DoubleSumAggregator.class);
		}
	}
}
