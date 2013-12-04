package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.cf.annotations.HyperParameter;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.jblas.FloatMatrix;

/**
 * Abstract class for all the ranking computation methods.
 * A bit less repetition of the code.
 * 
 * @author linas
 *
 */
public abstract class AbstractCFRankingComputation
		extends
        //CfLongId - node id (user or item)
        //FloatMatrixWritable - node value
        //FloatWritable - edge value (rating)
        //FloatMatrixMessage - message (sourceId, FloatMatrix, score)
        BasicComputation<CfLongId, FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {

	protected static final Logger logger = Logger.getLogger(BPRRankingComputation.class);
	
	int minItemId; //minimum item id in the system. Used for sampling the negative items.+
    int maxItemId; //maximum item id in the system

    static final FloatMatrixWritable emptyList = new FloatMatrixWritable(0);
    static final CfLongId nullId = new CfLongId();
    static final FloatMatrixMessage emptyMsg = new FloatMatrixMessage(nullId, emptyList, 0);

    float NOT_IMPORTANT = 0.0f;

    @HyperParameter(parameterName="dim", description="dimensionality of the model", defaultValue=10, minimumValue=1, maximumValue=1000)
    int d;

    @HyperParameter(parameterName="learnRate", description="learning rate", defaultValue=0.001f, minimumValue=0.0001f, maximumValue=10)
    float learnRate;

    @HyperParameter(parameterName="iter", description="number of iterations", defaultValue=10, minimumValue=1, maximumValue=1000)
    int iter;

    @HyperParameter(parameterName="reg", description="regularizer", defaultValue=0.01f, minimumValue=0.00011f, maximumValue=2)
    float reg;
	
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
			Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex,
			Iterable<FloatMatrixMessage> messages);
	
  @Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) throws IOException {


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
	            if (vertex.getId().isItem()){//only items
	                for (FloatMatrixMessage msg : messages) {
	                    applyUpdate(msg.getFactors(), vertex);
	                    sendMessage(msg.getSenderId(), emptyMsg);//just send something to user, that he would be present in the next computation
	                }
	            }
	        }
	    }else if(iteration == iter && getSuperstep() % 4 == 0){ //after all is computed
	        //now I have to send the last message to all the items and myself (user) in order to print out the results.
	        //if I don't do this step only user factors will be printed
	        if (vertex.getId().isItem()){
	            sendMessage(vertex.getId(), emptyMsg);
	            for (Edge<CfLongId, FloatWritable> edge : vertex.getEdges()) {
	                sendMessage(edge.getTargetVertexId(), emptyMsg);
	            }
	        }
	    }
	    vertex.voteToHalt();
	}

	
	protected void initFactorsIfNeeded(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {
		if (null == vertex.getValue() || vertex.getValue().columns != d){
			vertex.setValue(new FloatMatrixWritable(FloatMatrix.rand(d)));
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

	protected void sendRequestForFactors(CfLongId sendToItemId, CfLongId sentFromUserId, boolean relevant) {
        if (relevant){
            FloatMatrixMessage msgRelevant = new FloatMatrixMessage(sentFromUserId, emptyList, 1.0f);
            sendMessage(sendToItemId, msgRelevant);
            logger.debug(sentFromUserId+" ask for relevant factors to "+sendToItemId);
        }else{
            FloatMatrixMessage msgIRelevant = new FloatMatrixMessage(sentFromUserId, emptyList, 1.0f);
            sendMessage(sendToItemId, msgIRelevant);
            logger.debug(sentFromUserId+" ask for Irelevant factors to "+sendToItemId);
        }
    }

	
	void applyUpdate(FloatMatrix deltaUpdate, Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {
	    vertex.setValue(new FloatMatrixWritable(vertex.getValue().add(deltaUpdate)));
	}

	void sendItemFactorsUpdate(CfLongId itemId, CfLongId sendFrom, FloatMatrix factors) {
			    sendMessage(itemId, new FloatMatrixMessage(sendFrom, new FloatMatrixWritable(factors), NOT_IMPORTANT ));
	}

	protected boolean isRelevant(FloatMatrixMessage next) {
	    return next.getScore() > 0;
	}

	/**
	 * For all incomming messages send back my factors.
     * @param vertex
     * @param messages
     */
	void sendFactorsToUsers(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) {
	    if (vertex.getId().isItem()){
            FloatMatrixMessage msgRelevant = new FloatMatrixMessage(vertex.getId(), vertex.getValue(), 1.0f);//relevant
            FloatMatrixMessage msgIrrelevant = new FloatMatrixMessage(vertex.getId(), vertex.getValue(), -1.0f);
	        for (FloatMatrixMessage msg : messages) {
	            if (msg.getScore() > 0){
	                sendMessage(msg.getSenderId(), msgRelevant);
	            }else{
	                sendMessage(msg.getSenderId(), msgIrrelevant);
	            }
	        }
	    }
	}

	/**
	 * Sends request for factors for one relevant and one irrelevant item sampled uniformly over user items in the training set (relevant)
	 * and items that are not in the training set of the user (irrelevant). The sampling is a bit different that in the paper.
     * @param vertex
     */
	protected void sampleRelevantAndIrrelevantEdges(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {
		//FIXME fix the buffer to make the same function for all the methods
		int buffer = getBufferSize();
		
	    if (vertex.getId().isUser()){//only users
	        Random random = new Random();
	        Iterable<Edge<CfLongId, FloatWritable>> edges = vertex.getEdges();
	        ArrayList<CfLongId> itemList = new ArrayList<CfLongId>(vertex.getNumEdges());
	        HashSet<CfLongId> relevant = new HashSet<CfLongId>();

	        for (Edge<CfLongId, FloatWritable> e : edges) {
	            relevant.add(e.getTargetVertexId());
	            itemList.add(e.getTargetVertexId());
	        }
	        //relevant
	        CfLongId randomRelevantId = itemList.get(random.nextInt(itemList.size()));
	        //irrelevant
	
	        HashSet<CfLongId> randomIrrelevantIds = new HashSet<CfLongId>();
	
	        while(randomIrrelevantIds.size() < buffer)
	            randomIrrelevantIds.add(getRandomItemId(relevant));
	
	        //We use score > 0 to mark that this item is relevant, and score<0 to mark that it is irrelevant
	
	        sendRequestForFactors(randomRelevantId, vertex.getId(), true);
	        for(CfLongId irItemId: randomIrrelevantIds)  {
	            sendRequestForFactors(irItemId, vertex.getId(), false);
	        }
	    }
	}

    /**
     * Sample irrelevant items. This can be replaced by a simple sampling without checking relevant...
     * @param relevant
     * @return
     */
	protected CfLongId getRandomItemId(HashSet<CfLongId> relevant) {
	    Random r = new Random();
	    int top = (maxItemId-minItemId)+1;
	    long i = r.nextInt(top)+minItemId;
        CfLongId randId = new CfLongId((byte)1, i);
	    int maxCnt = 0;
	    while (relevant.contains(randId)){
	        i = r.nextInt(maxItemId-minItemId)+minItemId;
	        if (maxCnt > 1000000){//just to prevent an infinity loop
	            throw new RuntimeException("Can not sample a new irrelevant item");
	        }
	        maxCnt += 1;
            randId = new CfLongId((byte)1, i);
	    }
	    return randId;
	}

    static float logf(double x){
        return 1.0f/(1+(float)Math.exp(-x));
    }

    static float logfd(double x){
        return (float)Math.exp(x)/(float)(Math.pow(1+Math.exp(x),2));
    }
}