package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import ml.grafos.okapi.cf.annotations.HyperParameter;
import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
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
 * 
 * Optimizes Area Under the Curve (AUC) in CF setting. Port of myMediaLite implementation to the giraph framework.
 * 
 * Computes the BPR ranking adapted to giraph as discribed in:
 * S. Rendle, C. Freudenthaler, Z. Gantner, and S.-T. Lars. Bpr: Bayesian personalized ranking from implicit feedback. UAI �09, pages 452�461, 2009.
 * 
 * The main difference between original BPR and this implementation is that here sampling is done for each user, 
 * instead of random sample over the users and item triplets.
 * This leads to 2 main differences. 1) each user is updated at each iteration 2) for each iteration an item can be updated more than once.
 * In other worlds, item factors are updated in batches. For example,
 * imagine that we sample triplets in original BPR: (u1, i1, i2), (u2, i1, i3). After updates using first sample, values of i1 will change.
 * Therefore, when computing updates for 2 sample, the value of i1 is already new. In giraph implementation we would update these concurently,
 * therefore, for both we would use the same i1 factors and after iteration i1 would be updated 2 times with corresponding deltas.
 * 
 * Implementation algorithm:
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
 * @author linas
 *
 */

@OkapiAutotuning
public class BPRRankingComputation extends BasicComputation<LongWritable, DoubleArrayListWritable, IntWritable, LongDoubleArrayListMessage>{

	private static final DoubleArrayListWritable emptyList = new DoubleArrayListWritable();
	private static final LongDoubleArrayListMessage emptyMsg = new LongDoubleArrayListMessage(0, emptyList, 0);
	private double NOT_IMPORTANT = 0.0;
	
	protected static final Logger logger = Logger.getLogger(BPRRankingComputation.class);
	
	private int minItemId;
	private int maxItemId;
	
	@HyperParameter(parameterName="dim", description="dimensionality of the model", defaultValue=10, minimumValue=1, maximumValue=1000)
	private int d;
	
	@HyperParameter(parameterName="learnRate", description="learning rate", defaultValue=0.001f, minimumValue=0.0001f, maximumValue=10)
	private double learnRate;

	@HyperParameter(parameterName="iter", description="number of iterations", defaultValue=10, minimumValue=1, maximumValue=1000)
	private int iter;
	
	@HyperParameter(parameterName="reg", description="regularizer", defaultValue=0.01f, minimumValue=0.00011f, maximumValue=2)
	private double reg;
	
	@Override
	public void compute(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		
		long iteration = getSuperstep()/4;
		try{
			setConfigurationParameters();
		}catch(Exception e){
			e.printStackTrace();
		}
		
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
		if (vertex.getId().get() > 0){
			//each user should receive exactly 2 messages as he sent request to 2 items. One from relevant and one from irrelevant item;
			LongDoubleArrayListMessage i = null;
			LongDoubleArrayListMessage j = null;
			for (LongDoubleArrayListMessage msg : messages) {
				if (isRelevant(msg)){
					i = new LongDoubleArrayListMessage(msg);
				}else{
					j = new LongDoubleArrayListMessage(msg);
				}
			}
			//now do the magic computation with relevant and irrelevant and send the updates to items.
			updateModel(vertex.getValue(), i.getFactors(), i.getSenderId(), j.getFactors(), j.getSenderId(), vertex);
		}
	}
	
	/**
	 * Updates the model based on the factors received.
	 * It is a direct port from myMediaLite. 
	 * @param u
	 * @param i
	 * @param itemIid
	 * @param j
	 * @param itemJid
	 * @param vertex
	 */
	private void updateModel(DoubleArrayListWritable u, 
			DoubleArrayListWritable i, long itemIid, 
			DoubleArrayListWritable j, long itemJid, 
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		
		int ITEM_BIAS_INDEX = 0;
		
		double x_uij = i.get(ITEM_BIAS_INDEX).get() - j.get(ITEM_BIAS_INDEX).get() + rowScalarProductWithRowDifference(u, i, j);
		double one_over_one_plus_ex = 1 / (1 + Math.exp(x_uij));
		
		//compute item i and j bias terms
		double updateI = one_over_one_plus_ex - reg * i.get(ITEM_BIAS_INDEX).get();
		double updateJ = -one_over_one_plus_ex - reg * j.get(ITEM_BIAS_INDEX).get();
		double newIBias = (learnRate * updateI);
		double newJBias = (learnRate * updateJ);
		
		DoubleArrayListWritable uDelta = new DoubleArrayListWritable();
		DoubleArrayListWritable iDelta = new DoubleArrayListWritable();
		DoubleArrayListWritable jDelta = new DoubleArrayListWritable();
		uDelta.add(new DoubleWritable(0));
		iDelta.add(new DoubleWritable(newIBias));
		jDelta.add(new DoubleWritable(newJBias));
		
		// adjust factors
		for (int f = 1; f < d+1; f++){
			double w_uf = u.get(f).get();
			double h_if = i.get(f).get();
			double h_jf = j.get(f).get();
			double update = (h_if - h_jf) * one_over_one_plus_ex - reg * w_uf;
			uDelta.add(new DoubleWritable(learnRate * update));
			update = w_uf * one_over_one_plus_ex - reg * h_if;
			iDelta.add(new DoubleWritable(learnRate * update));
			update = -w_uf * one_over_one_plus_ex - reg * h_jf;
			jDelta.add(new DoubleWritable(learnRate * update));
		}
		//do the real update
		applyUpdate(uDelta, vertex);
		sendItemFactorsUpdate(itemIid, vertex.getId().get(), iDelta);
		sendItemFactorsUpdate(itemJid, vertex.getId().get(), jDelta);
		
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
	 * Compute the scalar product of a matrix row with the difference vector of two other matrix rows.
	 * Port from mymedialite.
	 * @param u
	 * @param i
	 * @param j
	 * @return
	 */
	private double rowScalarProductWithRowDifference(DoubleArrayListWritable u,
			DoubleArrayListWritable i, DoubleArrayListWritable j) {
		double res = 0;
		for (int k=1; k<u.size(); k++){//we skip the index 0 as it is reserved for item biases.
			res += u.get(k).get() * (i.get(k).get() - j.get(k).get());
		}
		return res;
	}

	private boolean isRelevant(LongDoubleArrayListMessage next) {
		return (next.getScore() > 0)? true: false;
	}

	/**
	 * For all incomming messages send back my factors.
	 * @param vertex
	 * @param messages
	 */
	private void sendFactorsToUsers(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex, Iterable<LongDoubleArrayListMessage> messages) {
		if (vertex.getId().get() < 0){//users are positives, items are negatives, 0 can not extist.
			LongDoubleArrayListMessage msgRelevant = new LongDoubleArrayListMessage(vertex.getId().get(), vertex.getValue(), 1.0);//relevant
			LongDoubleArrayListMessage msgIrrelevant = new LongDoubleArrayListMessage(vertex.getId().get(), vertex.getValue(), -1.0);
			for (LongDoubleArrayListMessage msg : messages) {
				if (msg.getScore() > 0){
					sendMessage(new LongWritable(msg.getSenderId()), msgRelevant);
					logger.debug(vertex.getId().get()+" sends relevant factors to "+msg.getSenderId());
				}else{
					sendMessage(new LongWritable(msg.getSenderId()), msgIrrelevant);
					logger.debug(vertex.getId().get()+" sends Irelevant factors to "+msg.getSenderId());
				}
			}
		}
	}
	
	public static <T> ArrayList<T> copyIterator(Iterable<T> iter) {
		Iterator<T> it = iter.iterator();
		ArrayList<T> copy = new ArrayList<T>();
	    while (it.hasNext())
	        copy.add(it.next());
	    return copy;
	}

	/**
	 * Sends request for factors for one relevant and one irrelevant item sampled uniformly over user items in the training set (relevant)
	 * and items that are not in the training set of the user (irrelevant). The sampling is a bit different that in the paper.
	 * @param vertex
	 */
	protected void sampleRelevantAndIrrelevantEdges(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		if (vertex.getId().get() > 0){//only users
			Random random = new Random();
			Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
			ArrayList<Long> itemList = new ArrayList<Long>(vertex.getNumEdges());
			HashSet<Long> relevant = new HashSet<Long>();
			for (Edge<LongWritable, IntWritable> e : edges) {
				relevant.add(e.getTargetVertexId().get());
				itemList.add(e.getTargetVertexId().get());
			}
			//relevant
			long randomRelevantId = itemList.get(random.nextInt(itemList.size()));
			//irrelevant
			long randomIrelevantItemId = getRandomItemId(relevant);
			//We use score > 0 to mark that this item is relevant, and score<0 to mark that it is irrelevant
			
			assert(randomRelevantId < 0);
			assert(randomIrelevantItemId < 0);
			
			sendRequestForFactors(randomRelevantId, vertex.getId().get(), true);
			sendRequestForFactors(randomIrelevantItemId, vertex.getId().get(), false);
		}
	}
	
	public int getMinItemId() {
		return minItemId;
	}

	public void setMinItemId(int minItemId) {
		this.minItemId = minItemId;
	}

	public int getMaxItemId() {
		return maxItemId;
	}

	public void setMaxItemId(int maxItemId) {
		this.maxItemId = maxItemId;
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

	protected long getRandomItemId(HashSet<Long> relevant) {
		Random r = new Random();
		int top = (maxItemId-minItemId)+1;
		long i = r.nextInt(top)+minItemId;
		int maxCnt = 0;
		while (relevant.contains(i)){
			i = r.nextInt((int)(maxItemId-minItemId))+minItemId;
			if (maxCnt > 1000000){//just to prevent infinity loop
				throw new RuntimeException("Can not sample a new irrelevant item");
			}
			maxCnt += 1;
		}
		return i;
	}
	
	private void setConfigurationParameters() throws NumberFormatException, IllegalArgumentException, IllegalAccessException {
		//required
		minItemId = Integer.parseInt(getConf().get("minItemId"));
		maxItemId = Integer.parseInt(getConf().get("maxItemId"));
		
		//optional (with defaults)
		for(Field field : this.getClass().getFields()){
			if (field.isAnnotationPresent(HyperParameter.class)){
				for(Annotation annotation : field.getDeclaredAnnotations()){
					if (annotation instanceof HyperParameter){
							HyperParameter hp = (HyperParameter)annotation;
							String param = getConf().get(hp.parameterName());
							if (null == param){
								param = ""+hp.defaultValue();
								logger.debug("Could not get parameter "+hp.parameterName()+" from the custom arguments, setting to the default."+hp.defaultValue());
							}
							if (field.getType() == int.class || field.getType() == Integer.class){
								field.setInt(this, Integer.parseInt(param));
							}else if (field.getType() == Float.class || field.getType() == float.class){
								field.setFloat(this, Float.parseFloat(param));
							}else if (field.getType() == Double.class || field.getType() == double.class){
								field.setDouble(this, Double.parseDouble(param));
							}else{
								throw new IllegalArgumentException("We support ints, floats and doubles as the parameters");
							}
					}
				}
			}
		}
	}
}
