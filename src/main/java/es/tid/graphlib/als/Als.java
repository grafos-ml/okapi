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
	//static int DECIMALS = 4;
	/** Number of updates */
	public int nupdates = 0;
	/** The most recent change in the factor value */
	//public double residual = 0d;
	/** Observed Value - Rating */
	private double observed = 0d;
	/** Error */    
	public double err = 0d;
	/** RMSE Error */
	private double rmseErr = 0d;
	/** Factor Error: it may be RMSE or L2NORM on initial&final vector  */
	public double halt_factor=0d;
	/** Initial vector value to be used for the L2Norm case */
	DoubleArrayListWritable initialValue = new DoubleArrayListWritable();
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
			/** Create table with neighbors latent values and ids */
			if (getSuperstep() == 1 || getSuperstep() == 2) {
				getValue().setNeighborValue(message.getSourceId(), message.getMessage());
				//for (int i=0; i<getValue().getAllNeighValue().size(); i++)
					//getValue().getNeighValue(i).
			}
			if (getSuperstep() > 2) {
				updateNeighValues(getValue().getNeighValue(message.getSourceId()), message.getMessage());
			}
		} // Eof Messages
		
		if (getSuperstep()>0) {
			for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()) {
				/** Calculate error */
				observed = (double)getEdgeValue(vvertex.getKey()).get();
				err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
				System.out.print("BEFORE: error = " + err + " vertex_vector= " + getValue().getLatentVector() + 
						" vv: " + vvertex.getKey() + ", ");
				vvertex.getValue().print();
			}
			runAlsAlgorithm();
			for (Entry<IntWritable, DoubleArrayListWritable> vvertex: getValue().getAllNeighValue().entrySet()) {
				err = getError(getValue().getLatentVector(), vvertex.getValue(), observed);
				System.out.println("AFTER: error = " + err + " vertex_vector= " + getValue().getLatentVector() + " vv: " + vvertex.getKey());
			}
			
			/** If termination flag is set to RMSD or RMSD aggregator is true */
			if (factorFlag.equals("rmse") || rmseFlag) {
				rmseErr+= Math.pow(err, 2);
			}
		} // Eof Superstep>0
		
		halt_factor = defineFactor(factorFlag, msgCounter);
		
		// If RMSE aggregator flag is true
		if (rmseFlag){
			this.aggregate(RMSE_AGG, new DoubleWritable(rmseErr));
		} 
		
		if (getSuperstep()==0 || (halt_factor > TOLERANCE && getSuperstep()<ITERATIONS)){
			sendMsgs();
		}
		// halt_factor is used in the OutputFormat file. --> To print the error
		if (factorFlag.equals("basic")){
			halt_factor=err;
		}
		voteToHalt();
	} // Eof compute()
	
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
			/** Store the latent vector of the current neighbor */
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
		
		DoubleMatrix matId = new DoubleMatrix();
		double reg = LAMBDA * getNumEdges();
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
		 *  Convert Amat and Vmat into type: DenseMatrix
		 *  in order to use the QRDecomposition method from Mahout
		 */
		DenseMatrix amat = doubleMatrix2Matrix(Amat, VECTOR_SIZE, VECTOR_SIZE);
		DenseMatrix vmat = doubleMatrix2Matrix(Vmat, VECTOR_SIZE, 1);
		Vector Umat = new QRDecomposition(amat).solve(vmat).viewColumn(0);
		
		//updateLatentVector(Umat);
		/*for (int i=0; i<VECTOR_SIZE; i++){
			getValue().setLatentVector(i, new DoubleWritable(getValue().getLatentVector().get(i)));
		}*/
		/** Update current vertex latent vector */
	
	    DoubleArrayListWritable val = new DoubleArrayListWritable();
	    for (int i=0; i<VECTOR_SIZE; i++){
	    	val.add(new DoubleWritable(Umat.get(i)));
	 	}
	 	getValue().setLatentVector(val);
		System.out.println("v: " + getValue().getLatentVector());
		nupdates++;
	}
	
	/*** Update current vertex latent vector */
	public void updateLatentVector(Vector value) {
		for (int i=0; i<VECTOR_SIZE; i++){
			getValue().setLatentVector(i, new DoubleWritable(value.get(i)));
		}
	}
	/*** Update neighbor's values */
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
	/*** Send messages to neighbors */
	/*public void sendMsgs() {
		*//** Send to all neighbors a message*/
		/*for (Edge<IntWritable, IntWritable> edge : getEdges()) {
			MessageWrapper message = new MessageWrapper();
			*/
			/* 1st superstep, users additionally send rating to items */
			/*if (getSuperstep() == 0) {
				message = wrapMessage(getId(), getValue().getLatentVector(), -1);
			}
			else {
				message = wrapMessage(getId(), getValue().getLatentVector(), edge.getValue().get());
			}

			sendMessage(edge.getTargetVertexId(), message);
			System.out.println("  [SEND] to " + edge.getTargetVertexId() + 
					" (rating: " + edge.getValue() + ")" +
					" [" + getValue().getLatentVector() + "]");
		} // Eof for each edge
	}*/
	
	/*** Create a message and wrap together the source id and the message (and rating if applicable) */
	public MessageWrapper wrapMessage(IntWritable id, DoubleArrayListWritable vector, int rating) {
		if (rating != -1) {
			vector.add(new DoubleWritable(rating));
		}
		return new MessageWrapper(id, vector);
	}
	
	/*** Calculate the RMSD on the errors calculated by the current vertex */
	public double getRMSE(int msgCounter){
		return Math.sqrt(rmseErr/msgCounter);
	}
	/*** Calculate the RMSE on the errors calculated by the current vertex */
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
		System.out.println("ma, mb");
		ma.print();
		mb.print();
		/** Convert ma,mb to DoubleMatrix 
		 *  in order to use the dot-product method from jblas library */
		DoubleMatrix matMa = DoubleArrayListWritable2DoubleMatrix(ma, ma.size());
		DoubleMatrix matMb = DoubleArrayListWritable2DoubleMatrix(mb, mb.size());
		System.out.println("ma, mb");
		matMa.print();
		matMb.print();
		/** Predicted value */
		double predicted = matMa.dot(matMb);
		predicted = Math.min(predicted, MAX);
		predicted = Math.max(predicted, MIN);
		System.out.println("predicted: " + predicted + " - observed: " + observed);
		return predicted-observed;
	}
	
	/*** Convert a DoubleMatrix (from jblas library) 
	 **  to DenseMatrix (from Mahout library) 
	 ***/
	public DenseMatrix doubleMatrix2Matrix(DoubleMatrix Amat, int x, int y) {
		double[][] amatDouble = new double[x][y];		
		for (int i=0; i<x; i++){
			for (int j=0; j<y; j++){
				amatDouble[i][j]=Amat.get(i, j);
			}
		}
		return new DenseMatrix(amatDouble);
	}
	/*** Convert a DoubleArrayListWritable (from graphlib library) 
	 **  to DoubleMatrix (from jblas library) 
	 ***/
	public DoubleMatrix DoubleArrayListWritable2DoubleMatrix(DoubleArrayListWritable ma, int size){
		DoubleMatrix matMa = new DoubleMatrix(size);
		
		for (int i=0; i<size; i++) {
			matMa.put(i, ma.get(i).get());
		}
		return matMa;
	}
	
	/*** Return type of current vertex */
	public boolean isItem(){
		return item;
	}

	/*** Return amount of vertex updates */
    public int getUpdates(){
    	return nupdates;
    }
    
	/*** Define whether the halt factor is "basic", "rmse" or "l2norm" */
	public double defineFactor(String factorFlag, int msgCounter){
		double factor=0d;
		switch (factorFlag) {
			case "basic": factor = TOLERANCE+1d; break;
			case "rmse": factor = getRMSE(msgCounter); break;
			case "l2norm": factor = getL2Norm(initialValue, getValue().getLatentVector()); break;
		} 
		return factor;
	}
}
