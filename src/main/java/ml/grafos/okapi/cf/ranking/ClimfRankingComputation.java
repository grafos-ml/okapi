package ml.grafos.okapi.cf.ranking;

import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;


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
        ClimfRankingComputation extends AbstractCFRankingComputation{

    protected static final Logger logger = Logger.getLogger(ClimfRankingComputation.class);

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
        uDelta.diff(u.mul(reg));
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
            vDelta.sum(V_j.mul(reg));
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


    @Override
    int getBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }
}

