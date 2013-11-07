package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.util.Random;


import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;


/**
 /**
 * The CLiMF ranking algorithm for Collaborative Filtering in the Giraph model.
 * 
 * The main difference between the original CLiMF and this implementation is that learning is happening in semi-batch mode i.e. all users
 * are updated and then all items are updated etc. this is due to the Giraph computing model.
 * This leads to 2 main differences. 1) each user is updated at each iteration 2) for each iteration an item can be updated more than once.
 * In other worlds, item factors are updated in batches. For example,
 * imagine that we sample triplets in original CLiMF: (u1, i1, i2), (u2, i1, i3). After updates using the first sample, the values of i1 will change.
 * Therefore, when computing updates for 2 sample, the value of i1 is already new. In the Giraph implementation we update these concurrently,
 * therefore, for both we would use the same i1 factors and after iteration i1 would be updated 2 times with corresponding deltas.
 * 
 * Implementation of the  algorithm:
 * The idea is to represent users and items as bipartite graph. Each node has its latent factors. Each edge has score (>0) or 0 if it is sampled as irrelevant.
 * 1. User samples the relevant items and same amount of irrelevant items. To sample irrelevant items we basically create edge with 0.
 * Then he asks all these items to send him their factors. 
 * 2. Item nodes don't do anything but send back their factors to requested user.
 * 3. User nodes compute prediction for all relevant and irrelevant items and compute update vectors based
 * on these predictions.
 * 4. User node updates itself with the computed gradient and send computed updates to the items. After sending
 * the updates to items it erases all edges to irrelevant items (marked as 0).
 * 5. Items update themselves. 
 * 6. Start from 1 for #iterations.
 * 
 * We make additional trick. We want the final model to be U*V and to remove all the item and user biases.
 * So if we have d=10, we make U and V vectors d=11. the U[0]=1 and V[0]=item_0_bias. In this case we
 * can use the same evaluation framework for all the methods.
 * 
 * 
 * @author Alexandros
 *
 */
public class
        ClimfRankingComputation extends BasicComputation<LongWritable, DoubleArrayListWritable, IntWritable, LongDoubleArrayListMessage>{
	
	private static final DoubleArrayListWritable emptyList = new DoubleArrayListWritable();
	private static final LongDoubleArrayListMessage emptyMsg = new LongDoubleArrayListMessage(0, emptyList, 0);
	
	protected static final Logger logger = Logger.getLogger(ClimfRankingComputation.class);
	
	private int d; //dimensionality of the model
	private double learnRate; //learning rate
	private double userReg;
	private int iter; //number of iterations
	private double NOT_IMPORTANT = 0.0;
	
	
	@Override
	public void compute(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		
		long iteration = getSuperstep()/4;
		setConfigurationParameters();
		
		if (iteration < iter){
			if (getSuperstep() % 4 == 0){ //initial cycle of iteration where user samples and asks for factors
				initFactorsIfNeeded(vertex);
				sampleRelevantAndIrrelevantEdges(vertex);
			}else if (getSuperstep() % 4 == 1){ //items send factors to the user
				initFactorsIfNeeded(vertex);
				sendFactorsToUsers(vertex, messages);
			}else if (getSuperstep() % 4 == 2){ //users compute the updates and updates itself
				computeModelUpdates(vertex, messages);
			}else if (getSuperstep() % 4 == 3){ //items update themselves
				if (vertex.getId().get() < 0){//only items
					for (LongDoubleArrayListMessage msg : messages) {
						applyUpdate(msg.getFactors(), vertex);
						sendMessage(new LongWritable(msg.getSenderId()), emptyMsg);//just send something to user, that he would be present in the next computation
					}
				}
			}
		}else if(iteration == iter && getSuperstep() % 4 == 0){ //after all is computed
			//now I have to send the last message to all the items and myself (user) in order to print out the results.
			//if I don't do this step only user factors will be printed
			if (vertex.getId().get() > 0){
				sendMessage(vertex.getId(), emptyMsg);
				for (Edge<LongWritable, IntWritable> edge : vertex.getEdges()) {
					sendMessage(edge.getTargetVertexId(), emptyMsg);
				}
			}
		}
		vertex.voteToHalt();
	}
	
	
	private void initFactorsIfNeeded(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		if (vertex.getValue().size() != d+1){
			Random r = new Random();
			DoubleArrayListWritable randomValueVector = new DoubleArrayListWritable();
			for (int i=0; i<this.d+1; i++){
				if (i==0 && vertex.getId().get() > 0){ //for user first value is always 1, for items it is a item bias
					randomValueVector.add(new DoubleWritable(1.0));
				}else{
					randomValueVector.add(new DoubleWritable(r.nextDouble()*0.01));
				}
			}
			vertex.setValue(randomValueVector);
		}
	}

	public void computeModelUpdates(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) {
			//now do the magic computation with relevant and send the updates to items.
		if (vertex.getId().get() > 0) // only for users
			updateModel(vertex.getValue(), messages, vertex);
		}
	
	
	/**
	 * Updates the model based on the factors received.
	 * @param u   Model parameters for user u
	 * @param vertex  Current user vertex
	 */
	private void updateModel(DoubleArrayListWritable u, 
			Iterable<LongDoubleArrayListMessage> messages,
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
			//Compute User update 
			double tempdiff;
			DoubleArrayListWritable uDelta = DoubleArrayListWritable.zeros(u.size());
			DoubleArrayListWritable partialOneDelta;
			DoubleArrayListWritable partialTwoDelta = new DoubleArrayListWritable();
	
			for (LongDoubleArrayListMessage msg : messages) {
			DoubleArrayListWritable V_j = msg.getFactors();
			double fij = u.dot(V_j);
			partialOneDelta = V_j.mul(logf(-1.0*fij));  
			for (LongDoubleArrayListMessage msginner : messages) {
			    DoubleArrayListWritable V_k = msginner.getFactors();		
				tempdiff = fij - u.dot(V_k);
			    partialTwoDelta = V_j.diff(V_k).mul(logfd(tempdiff)/(1-logf(tempdiff)));
				}
				uDelta.sum(partialOneDelta);
				uDelta.sum(partialTwoDelta);
				
			}
			uDelta.diff(u.mul(userReg));
			uDelta.mul(learnRate);
		
			//Compute Item Updates
			for (LongDoubleArrayListMessage msg : messages) {
				DoubleArrayListWritable vDelta = DoubleArrayListWritable.zeros(u.size());
				DoubleArrayListWritable V_j = msg.getFactors();
				long Itemid = msg.getSenderId();
				double fij = u.dot(V_j);
				double partialSumOne = logf(-1.0*fij);
				double partialSumTwo = 0;
				for (LongDoubleArrayListMessage msginner : messages) {
				    DoubleArrayListWritable V_k = msginner.getFactors();		
					tempdiff = fij - u.dot(V_k);
					partialSumTwo += logfd(tempdiff)*(1.0/(1.0-logf(-1.0*tempdiff)) - 1.0/(1.0 - logf(tempdiff) ) );
				}
				
				vDelta.sum(u.mul(partialSumTwo + partialSumOne));
				vDelta.sum(V_j.mul(userReg));
				vDelta.mul(learnRate);
				
				sendItemFactorsUpdate(Itemid, vertex.getId().get(), vDelta);				
			}
			
		//do the user update
		applyUpdate(uDelta, vertex);
	}
	
	private static double logf(double x){
		return 1./(1+Math.exp(-x));
	}
	
	private static double logfd(double x){
		return Math.exp(x)/(Math.pow(1+Math.exp(x),2));
	}

	
	protected void applyUpdate(DoubleArrayListWritable deltaUpdate, Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex){
		DoubleArrayListWritable current = vertex.getValue();
		DoubleArrayListWritable updated = new DoubleArrayListWritable();
		for(int i=0; i<current.size(); i++){
			updated.add(new DoubleWritable(current.get(i).get() + deltaUpdate.get(i).get()));
		}
		vertex.setValue(updated);
	}
	
	protected void sendItemFactorsUpdate(long itemId, long sendFrom, DoubleArrayListWritable factors){
		sendMessage(new LongWritable(itemId), new LongDoubleArrayListMessage(sendFrom, factors, NOT_IMPORTANT ));
	}


	/**
	 * For all incomming messages send back my factors.
	 * @param vertex  current user and item vertices, only items send factors here
	 * @param messages  sender id
	 */
	private void sendFactorsToUsers(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex, Iterable<LongDoubleArrayListMessage> messages) {
		if (vertex.getId().get() < 0){//users are positives, items are negatives, 0 can not exist.
			LongDoubleArrayListMessage msgRelevant = new LongDoubleArrayListMessage(vertex.getId().get(), vertex.getValue(), 1.0);//relevant
			LongDoubleArrayListMessage msgIrrelevant = new LongDoubleArrayListMessage(vertex.getId().get(), vertex.getValue(), -1.0);
			for (LongDoubleArrayListMessage msg : messages) {
				if (msg.getScore() > 0){
					sendMessage(new LongWritable(msg.getSenderId()), msgRelevant);
					logger.debug(vertex.getId().get()+" sends relevant factors to "+msg.getSenderId());
				}else{
					sendMessage(new LongWritable(msg.getSenderId()), msgIrrelevant);
					logger.debug(vertex.getId().get()+" sends Irrelevant factors to "+msg.getSenderId());
				}
			}
		}
	}
	


	/**
	 * Sends request for factors for one relevant and one irrelevant item sampled uniformly over user items in the training set (relevant)
	 * and items that are not in the training set of the user (irrelevant). The sampling is a bit different that in the paper.
	 * @param vertex  current vertex, only users sample relevant items.
	 */
	protected void sampleRelevantAndIrrelevantEdges( //get relevant item factors for each user
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		if (vertex.getId().get() > 0){//only users
			Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
			for (Edge<LongWritable, IntWritable> e : edges) 
				sendRequestForFactors(e.getTargetVertexId().get(), vertex.getId().get(), true);			
		}
	}
	


	protected void sendRequestForFactors(long sendToItemId, long sentFromUserId, boolean relevant){
		if (relevant){
			LongDoubleArrayListMessage msgRelevant = new LongDoubleArrayListMessage(sentFromUserId, emptyList, 1.0);
			sendMessage(new LongWritable(sendToItemId), msgRelevant);
			logger.debug(sentFromUserId+" ask for relevant factors to "+sendToItemId);
		}else{
			LongDoubleArrayListMessage msgIRelevant = new LongDoubleArrayListMessage(sentFromUserId, emptyList, -1.0);
			sendMessage(new LongWritable(sendToItemId), msgIRelevant);
			logger.debug(sentFromUserId+" ask for Irelevant factors to "+sendToItemId);
		}
		
	}
	
	private void setConfigurationParameters() {
		
		//optional (with defaults)
		iter = Integer.parseInt(getConf().get("iter", "100"));
		d = Integer.parseInt(getConf().get("dim", "10"));
		userReg = Double.parseDouble(getConf().get("regUser", "0.01"));
		learnRate = Double.parseDouble(getConf().get("learnRate", "0.005"));
	}
}

