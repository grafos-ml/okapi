package ml.grafos.okapi.cf.ranking;

import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;


/**
 * Optimizes Mean Average Precision (MAP) in the CF setting.
 *
 * Computes the TFMAP ranking adapted to giraph as discribed in:
 * Y. Shi, A. Karatzoglou, L. Baltrunas, M. Larson, A. Hanjalic, and N. Oliver. 
 * TFMAP: optimizing MAP for top-n context-aware recommendation. In Proc. of SIGIR ’12, 2012.
 *
 * The main difference between original TFMAP and this implementation is that we totally omit context.
 * This is done in order to make unified evaluation for all the methods. Probably we should call it MFMAP :)
 *
 *
 * @author linas
 *
 */
@OkapiAutotuning
public class TFMAPRankingComputation extends AbstractCFRankingComputation{

    protected static final Logger logger = Logger.getLogger(TFMAPRankingComputation.class);

    private int bufferSize;//buffer size (irelevant + relevant)
    private double NOT_IMPORTANT = 0.0; //just a marker, that it is not important what we put as a value and it will not be used.

    public void computeModelUpdates(
            Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
            Iterable<LongDoubleArrayListMessage> buffer) {
        if (isUser(vertex)){
            //each user should receive exactly 2K messages, where K is the number of relevant items.

            //1. I will update the user factors using the relevant items.
            DoubleArrayListWritable updateUser = updateUser(buffer, vertex);
            applyUpdate(updateUser.mul(learnRate), vertex);

            //2. I will compute updates for the items and send the updates to the items to update themselves.
            for (LongDoubleArrayListMessage msg : buffer) {
                DoubleArrayListWritable update = updateItem(msg.getFactors(), vertex.getValue(), buffer);
                sendItemFactorsUpdate(msg.getSenderId(), vertex.getId().get(), update);
            }
        }
    }

    private DoubleArrayListWritable updateItem(DoubleArrayListWritable V_i,
                                               DoubleArrayListWritable U_m,
                                               Iterable<LongDoubleArrayListMessage> buffer) {

        int y_mi = 0; //replacement for sum{y_{mi}}
        double f_mi = U_m.dot(V_i);
        double buffer_sum = 0;
        for (LongDoubleArrayListMessage msgJ : buffer) {
            if (isRelevant(msgJ)){
                y_mi++;
                DoubleArrayListWritable V_j = msgJ.getFactors();
                buffer_sum += logfd(f_mi) * logf(dot_of_difference(U_m, V_j, V_i)) + ( logf(U_m.dot(V_j)) - logf(f_mi) * logfd(dot_of_difference(U_m, V_j, V_i)));
            }
        }
        DoubleArrayListWritable newV_i = U_m.mul(buffer_sum/y_mi).sum(V_i.mul(-reg));
        return newV_i;
    }

    /**
     *
     * @param buffer contains relevant and irrelevant items.
     * @param vertex
     * @return
     */
    private DoubleArrayListWritable updateUser(Iterable<LongDoubleArrayListMessage> buffer,
                                               Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
        int y_mi = 0; //the counter \sum_i^M{y_{mi}}
        DoubleArrayListWritable U_m = vertex.getValue();//current U_m factors
        DoubleArrayListWritable newU = DoubleArrayListWritable.zeros(U_m.size());//the new U_m factor
        for (LongDoubleArrayListMessage msgI : buffer) {//first sum;
            if (isRelevant(msgI)){//replacement of y_{mi}
                y_mi++;
                DoubleArrayListWritable V_i = msgI.getFactors();
                DoubleArrayListWritable deltaV_i = V_i.mul(computeDelta(U_m, V_i, buffer));//[\delta \elementwise V_i
                double gf_mi = logf(U_m.dot(V_i)); //g(f_{mi})
                DoubleArrayListWritable V_sum = DoubleArrayListWritable.zeros(V_i.size()); //where we store the second sum which runs over j
                for (LongDoubleArrayListMessage msgJ : buffer) {
                    if (isRelevant(msgJ)){//replacement of y_{mj}
                        DoubleArrayListWritable V_j = msgJ.getFactors();
                        V_sum = V_sum.sum(V_j.mul(logfd( dot_of_difference(U_m, V_j, V_i)) ));
                    }
                }
                newU = deltaV_i.sum(V_sum.mul(gf_mi));
            }
        }
        return newU.mul(1.0/y_mi).sum(U_m.mul(-reg));
    }

    /**
     * Substitution for delta abotve eq.8.
     * @param v_i
     * @param messages
     * @return
     */
    private double computeDelta(DoubleArrayListWritable m_i, DoubleArrayListWritable v_i,
                                Iterable<LongDoubleArrayListMessage> messages) {
        double f_mi = m_i.dot(v_i);
        double first = 0;
        double second = 0;
        for (LongDoubleArrayListMessage msgJ : messages) {
            if(isRelevant(msgJ)){
                DoubleArrayListWritable v_j = msgJ.getFactors();
                first += logf(dot_of_difference(m_i, v_j, v_i));
                second += logfd(dot_of_difference(m_i, v_j, v_i));
            }
        }
        return logfd(f_mi) * first + logf(f_mi) * second;
    }

    private double dot_of_difference(DoubleArrayListWritable m_i,
                                     DoubleArrayListWritable v_j, DoubleArrayListWritable v_i) {
        return m_i.dot(v_j) - m_i.dot(v_i);
    }

    public boolean isItem(
            Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
        return vertex.getId().get() < 0;
    }

    /**
     * Sends request for factors for one relevant and one irrelevant item sampled uniformly over user items in the training set (relevant)
     * and items that are not in the training set of the user (irrelevant). The sampling is a bit different that in the paper.
     * @param vertex
     */
    protected void sampleRelevantAndIrrelevantEdges(
            Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex, int k) {
        if (k < 1)
            throw new IllegalArgumentException("Buffer size should be >= 1");

        if (isUser(vertex)){//only users
            Random random = new Random();
            Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
            ArrayList<Long> itemList = new ArrayList<Long>(vertex.getNumEdges());
            HashSet<Long> allRelevant = new HashSet<Long>();
            for (Edge<LongWritable, IntWritable> e : edges) {
                allRelevant.add(e.getTargetVertexId().get());
                itemList.add(e.getTargetVertexId().get());
            }

            HashSet<Long> relevant = new HashSet<Long>();
            HashSet<Long> iRelevant = new HashSet<Long>();
            //relevant
            int cnt = 0;
            while(relevant.size() < k && relevant.size() < allRelevant.size()){
                long randomRelevantId = itemList.get(random.nextInt(itemList.size()));
                relevant.add(randomRelevantId);
                if (cnt > 1000000){
                    throw new RuntimeException("Can not sample a new relevant item because there are no more left! "+relevant.size() + " "+vertex.getId());
                }
                cnt++;
            }
            //irrelevant
            cnt = 0;
            while(iRelevant.size() < k){
                long randomIrelevantItemId = getRandomItemId(allRelevant);
                iRelevant.add(randomIrelevantItemId);
                if (cnt > 1000000){
                    throw new RuntimeException("Can not sample a new irrelevant item because there are no more left! "+iRelevant.size() + " "+vertex.getId());
                }
                cnt++;

            }

            for (Long itemId : relevant) {
                sendRequestForFactors(itemId, vertex.getId().get(), true);
            }
            for (Long itemId : iRelevant) {
                sendRequestForFactors(itemId, vertex.getId().get(), false);
            }
        }
    }


//	protected void sampleRelevantAndIrrelevantEdges(
//			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex) {
//		if (isUser(vertex)){//only users
//			logger.debug("Sampling from vertex"+vertex);
//			Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
//			HashSet<Long> relevant = new HashSet<Long>();
//			HashSet<Long> iRelevant = new HashSet<Long>();
//			for (Edge<LongWritable, IntWritable> e : edges) {
//				relevant.add(e.getTargetVertexId().get());
//			}
//			//irrelevant
//			int cnt = 0;
//			while(iRelevant.size() < relevant.size()){
//				long randomIrelevantItemId = getRandomItemId(relevant);
//				iRelevant.add(randomIrelevantItemId);
//				cnt++;
//				if (cnt > 1000000){
//					break;//for the users who has more than half movies watched
//					//throw new RuntimeException("Can not sample a new irrelevant item because there are no more left! "+relevant.size() + " "+vertex.getId());
//				}
//			}
//			//We use score > 0 to mark that this item is relevant, and score<0 to mark that it is irrelevant
//
//			for (Long itemId : relevant) {
//				sendRequestForFactors(itemId, vertex.getId().get(), true);
//			}
//			for (Long itemId : iRelevant) {
//				sendRequestForFactors(itemId, vertex.getId().get(), false);
//			}
//		}
//	}

    private static double logf(double x){
        return 1./(1+Math.exp(-x));
    }

    private static double logfd(double x){
        return Math.exp(x)/(Math.pow(1+Math.exp(x),2));
    }

    @Override
    int getBufferSize() {
        return bufferSize;
    }

}
