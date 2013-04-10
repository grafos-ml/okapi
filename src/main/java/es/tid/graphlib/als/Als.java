package es.tid.graphlib.als;


import java.util.Map.Entry;

import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.vertex.EdgeListVertex;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.mahout.math.QRDecomposition;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Vector;

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
	/** RMSE Error */
	private double rmseErr = 0d;
	/** Factor Error: it may be RMSE or L2NORM on initial&final vector  */
	public double err_factor=0d;
	/** Type of vertex
	 * 0 for user, 1 for item */
	private boolean item = false;
	/** Aggregator to get values from the workers to the master */
	public static final String RMSE_AGG = "rmse.aggregator";
	
	public void compute(Iterable<MessageWrapper> messages) {
		/** Counter of messages received - different from getNumEdges() 
		 * because a neighbor may not send a message */
		int msgCounter = 0;
		/** Flag for checking if parameter for RMSE aggregator received */
		boolean rmseFlag = getContext().getConfiguration().getBoolean("als.aggregate", false);
		/** Flag for checking which termination factor to use: basic, rmse, l2norm */
		String factorFlag = getContext().getConfiguration().get("als.factor", "basic");
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
				for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()){
					/** Calculate error */
					observed = (double)getEdgeValue(vvertex.getKey()).get();
					err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
					System.out.println("BEFORE: error = " + err + " vertex_vector= " + getValue().getLatentVector());
				}
				runAlsAlgorithm();
				for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()){
					observed = (double)getEdgeValue(vvertex.getKey()).get();
					err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
					System.out.println("AFTER: error = " + err + " vertex_vector= " + getValue().getLatentVector());
				}
				
				/** If termination flag is set to RMSD or RMSD aggregator is true */
				if (factorFlag.equals("rmse") || rmseFlag) {
					rmseErr+= Math.pow(err, 2);
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

    /*** Update Vertex Latent Vector based on ALS equation
     * Amat = MiIi * t(MiIi) + LAMBDA * Nui * E
	 * Vmat = MiIi * t(R(i,Ii))
	 * Amat * Umat = Vmat <==> solve Umat
	 *  
	 * where MiIi: movies feature matrix rated by user i (matNeighVectors)
	 * 		 t(MiIi): transpose of MiIi (matNeighVectorsTrans)
	 * 		 Nui: number of ratings of user i (getNumEdges())
	 * 		 E: identity matrix (matId)
	 * 		 R(i,Ii): ratings of movies rated by user i
	 */
	public void runAlsAlgorithm(){
		int j=0;
		DoubleMatrix matNeighVectors = new DoubleMatrix(VECTOR_SIZE,getNumEdges());
		double[] curVec = new double[VECTOR_SIZE];
		DoubleMatrix ratings = new DoubleMatrix(getNumEdges());
		/** Go through the neighbors */
		for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()){
			/* Store the latent vector of the current neighbor */
			for (int i=0; i<VECTOR_SIZE;i++){
				curVec[i]=vvertex.getValue().get(i).get();
			}
			matNeighVectors.putColumn(j, new DoubleMatrix(curVec));
			
			/* Store the rating related with the current neighbor */
			ratings.put(j, (double)getEdgeValue(vvertex.getKey()).get());
			j++;
		} // Eof Neighbours Edges
		
		/** Amat = MiIi * t(MiIi) + LAMBDA * getNumEdges() * matId */
		System.out.println("matNeigh * matNeighTrans = matMul");
		DoubleMatrix matNeighVectorsTrans = matNeighVectors.transpose();
		DoubleMatrix matMul = matNeighVectors.mmul(matNeighVectorsTrans);
		matNeighVectors.print();
		matNeighVectorsTrans.print();
		matMul.print();
		
		double reg = LAMBDA * getNumEdges();
		DoubleMatrix matId = new DoubleMatrix();
		matId = matId.eye(VECTOR_SIZE);
		System.out.println("matMul + (reg=" + reg + " * matId)");
		matMul.print();
		matId.print();
		DoubleMatrix Amat = matMul.add(matId.mul(reg));
		
		/** Vmat = MiIi * t(R(i,Ii)) */
		System.out.println("matNeigh * transpose(ratings)");
		matNeighVectors.print();
		ratings.print();
		DoubleMatrix Vmat = matNeighVectors.mmul(ratings);
		
		System.out.println("Amat, Vmat");
		Amat.print();
		Vmat.print();
		
		/** Amat * Umat = Vmat <==> solve Umat
		 *  Convert Amat and Vmat into type: Matrix of Mahout 
		 */
		double[][] amatDouble = new double[VECTOR_SIZE][VECTOR_SIZE];		
		for (int i=0; i<VECTOR_SIZE; i++){
			for (j=0; j<VECTOR_SIZE; j++){
				amatDouble[i][j]=Amat.get(i, j);
			}
		}
		DenseMatrix amat = new DenseMatrix(amatDouble);
		
		double[][] vmatDouble = new double[VECTOR_SIZE][1];
		for (int i=0; i<VECTOR_SIZE; i++){
			vmatDouble[i][0] = Vmat.get(i,0);
		}
		DenseMatrix vmat = new DenseMatrix(vmatDouble);
		
		Vector Umat = new QRDecomposition(amat).solve(vmat).viewColumn(0);
		
		/** Update current vertex latent vector */
		DoubleArrayListWritable val = new DoubleArrayListWritable();
		for (int i=0; i<VECTOR_SIZE; i++){
			val.add(new DoubleWritable(Umat.get(i)));
		}
		getValue().setLatentVector(val);

		nupdates++;
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
