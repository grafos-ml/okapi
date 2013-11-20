package ml.grafos.okapi.cf.ranking;

import ml.grafos.okapi.cf.annotations.HyperParameter;
import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

/**
 * Abstract class for all the ranking computation methods.
 * A bit less repetition of the code.
 * 
 * @author linas
 *
 */
public abstract class AbstractCFRankingComputation
		extends
		BasicComputation<LongWritable, DoubleArrayListWritable, IntWritable, LongDoubleArrayListMessage> {

	protected static final Logger logger = Logger.getLogger(BPRRankingComputation.class);
	
	int minItemId; //minimum item id in the system. Used for sampling the negative items.+
    int maxItemId; //maximum item id in the system
    
    static final DoubleArrayListWritable emptyList = new DoubleArrayListWritable();
    static final LongDoubleArrayListMessage emptyMsg = new LongDoubleArrayListMessage(0, emptyList, 0);
    double NOT_IMPORTANT = 0.0;

    @HyperParameter(parameterName="dim", description="dimensionality of the model", defaultValue=10, minimumValue=1, maximumValue=1000)
    int d;

    @HyperParameter(parameterName="learnRate", description="learning rate", defaultValue=0.001f, minimumValue=0.0001f, maximumValue=10)
    double learnRate;

    @HyperParameter(parameterName="iter", description="number of iterations", defaultValue=10, minimumValue=1, maximumValue=1000)
    int iter;

    @HyperParameter(parameterName="reg", description="regularizer", defaultValue=0.01f, minimumValue=0.00011f, maximumValue=2)
    double reg;
	
	/**
	 * The function \\FIXME add doc
	 */
	public AbstractCFRankingComputation() {
		super();
	}
	
	/**
	 * The buffer size depends on the method.
	 * @return
	 */
	abstract int getBufferSize();
	
	/**
	 * This is the main function for each Okapi CF ranking method.
	 * Based on this update function, the method will differ from other methods.
	 * @param vertex
	 * @param messages
	 */
	abstract void computeModelUpdates(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages);
	
	@Override
	public void compute(Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex, Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		setConfigurationParameters();
		
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

	
	protected void initFactorsIfNeeded(Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		if (vertex.getValue().size() != d){
			Random r = new Random();
			DoubleArrayListWritable randomValueVector = new DoubleArrayListWritable();
			for (int i=0; i<this.d; i++){
				if (i==0 && vertex.getId().get() > 0){ //for user first value is always 1, for items it is a item bias
					randomValueVector.add(new DoubleWritable(1.0));
				}else{
					randomValueVector.add(new DoubleWritable(r.nextDouble()*0.01));
				}
			}
			vertex.setValue(randomValueVector);
		}
	}

	protected void setConfigurationParameters()
			throws IOException {
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
									try{
										if (field.getType() == int.class || field.getType() == Integer.class){
											field.setInt(this, Integer.parseInt(param));
										}else if (field.getType() == Float.class || field.getType() == float.class){
											field.setFloat(this, Float.parseFloat(param));
										}else if (field.getType() == Double.class || field.getType() == double.class){
											field.setDouble(this, Double.parseDouble(param));
										}else{
											throw new IOException("We support ints, floats and doubles as the parameters");
										}
									}catch (Exception e) {
										throw new IOException(e);
									}
							}
						}
					}
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

	protected void sendRequestForFactors(long sendToItemId, long sentFromUserId,
			boolean relevant) {
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

	
	void applyUpdate(DoubleArrayListWritable deltaUpdate, Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
	    DoubleArrayListWritable updated = vertex.getValue().sum(deltaUpdate);
	    vertex.setValue(updated);
	}

	void sendItemFactorsUpdate(long itemId, long sendFrom,
			DoubleArrayListWritable factors) {
			    sendMessage(new LongWritable(itemId), new LongDoubleArrayListMessage(sendFrom, factors, NOT_IMPORTANT ));
	}

	protected boolean isRelevant(LongDoubleArrayListMessage next) {
	    return next.getScore() > 0;
	}

	/**
	 * For all incomming messages send back my factors.
	 * @param vertex
	 * @param messages
	 */
	void sendFactorsToUsers(Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex, Iterable<LongDoubleArrayListMessage> messages) {
	    if (vertex.getId().get() < 0){//users are positives, items are negatives, 0 cannot exist.
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
	 * @param vertex
	 */
	protected void sampleRelevantAndIrrelevantEdges(Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
		//FIXME fix the buffer to make the same function for all the methods
		int buffer = getBufferSize();
		
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
	
	        HashSet<Long> randomIrrelevantIds = new HashSet<Long>();
	
	        while(randomIrrelevantIds.size() < buffer)
	            randomIrrelevantIds.add(getRandomItemId(relevant));
	
	        //We use score > 0 to mark that this item is relevant, and score<0 to mark that it is irrelevant
	
	        assert(randomRelevantId < 0);
	
	        sendRequestForFactors(randomRelevantId, vertex.getId().get(), true);
	        for(Long irItemId: randomIrrelevantIds)  {
	            assert(irItemId < 0);
	            sendRequestForFactors(irItemId, vertex.getId().get(), false);
	        }
	    }
	}

	public boolean isUser(Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
	    return vertex.getId().get() > 0;
	}

	protected long getRandomItemId(HashSet<Long> relevant) {
	    Random r = new Random();
	    int top = (maxItemId-minItemId)+1;
	    long i = r.nextInt(top)+minItemId;
	    int maxCnt = 0;
	    while (relevant.contains(i)){
	        i = r.nextInt(maxItemId-minItemId)+minItemId;
	        if (maxCnt > 1000000){//just to prevent an infinity loop
	            throw new RuntimeException("Can not sample a new irrelevant item");
	        }
	        maxCnt += 1;
	    }
	    return i;
	}

}