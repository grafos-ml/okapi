/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.cf.ranking;

import java.util.ArrayList;
import java.util.Iterator;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.jblas.FloatMatrix;


/**
 *
 * Optimizes Area Under the Curve (AUC) in CF setting. Port of myMediaLite implementation to the giraph framework.
 *
 * Computes the BPR ranking adapted to giraph as discribed in:
 * S. Rendle, C. Freudenthaler, Z. Gantner, and S.-T. Lars. Bpr: Bayesian personalized ranking from implicit feedback. UAI ’09, pages 452–461, 2009.
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
public class BPRRankingComputation extends AbstractCFRankingComputation{

    protected final Logger logger = Logger.getLogger(BPRRankingComputation.class);

    public void computeModelUpdates(
            Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex,
            Iterable<FloatMatrixMessage> messages) {
        if (vertex.getId().isUser()){
            //each user should receive exactly 2 messages as he sent request to 2 items. One from relevant and one from irrelevant item;
            FloatMatrixMessage i = null;
            FloatMatrixMessage j = null;
            for (FloatMatrixMessage msg : messages) {
                if (isRelevant(msg)){
                    i = new FloatMatrixMessage(msg);
                }else{
                    j = new FloatMatrixMessage(msg);
                }
            }
            //now do the magic computation with relevant and irrelevant and send the updates to items.
            updateModel(vertex.getValue(), i.getFactors(), i.getSenderId(), j.getFactors(), j.getSenderId(), vertex);
        }
    }


    /**
     * We override this function as we need a special treatment for item biases. See the class documentation for the explanation.
     */
    protected void initFactorsIfNeeded(Vertex<CfLongId,FloatMatrixWritable,FloatWritable> vertex) {
        if (null == vertex.getValue() || vertex.getValue().columns != d+1){
            vertex.setValue(new FloatMatrixWritable(FloatMatrix.rand(d + 1)));
        }
        if (vertex.getId().isUser()){//In BPR the first factor of the user is always 1, its to have item baselines
            vertex.getValue().put(0, 1f);
        }
    }

    /**
     * Updates the model based on the factors received.
     * It is a direct port from myMediaLite.
     * @param u - column vector of user factors
     * @param i - column vector of item 1 factors
     * @param itemIid 
     * @param j - column vector of item 2 factors
     * @param itemJid
     * @param vertex
     */
    private void updateModel(FloatMatrix u,
                             FloatMatrix i, CfLongId itemIid,
                             FloatMatrix j, CfLongId itemJid,
                             Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {

        int ITEM_BIAS_INDEX = 0;

        float x_uij = i.get(ITEM_BIAS_INDEX) - j.get(ITEM_BIAS_INDEX) + rowScalarProductWithRowDifference(u, i, j);
        float one_over_one_plus_ex = 1 / (1 + (float)Math.exp(x_uij));

        //compute item i and j bias terms
        float updateI = one_over_one_plus_ex - reg * i.get(ITEM_BIAS_INDEX);
        float updateJ = -one_over_one_plus_ex - reg * j.get(ITEM_BIAS_INDEX);
        float newIBias = (learnRate * updateI);
        float newJBias = (learnRate * updateJ);

        FloatMatrix uDelta = FloatMatrix.zeros(u.rows);
        FloatMatrix iDelta = FloatMatrix.zeros(u.rows);
        FloatMatrix jDelta = FloatMatrix.zeros(u.rows);
        uDelta.put(ITEM_BIAS_INDEX, 0); //because it is update for user, it should never update 1 into something else. therefore 0.
        iDelta.put(ITEM_BIAS_INDEX, newIBias);
        jDelta.put(ITEM_BIAS_INDEX, newJBias);

        // adjust factors
        for (int f = 1; f < d+1; f++){
            float w_uf = u.get(f);
            float h_if = i.get(f);
            float h_jf = j.get(f);
            float update = (h_if - h_jf) * one_over_one_plus_ex - reg * w_uf;
            uDelta.put(f, learnRate * update);
            update = w_uf * one_over_one_plus_ex - reg * h_if;
            iDelta.put(f, learnRate * update);
            update = -w_uf * one_over_one_plus_ex - reg * h_jf;
            jDelta.put(f, learnRate * update);
        }
        //do the real update
        applyUpdate(uDelta, vertex);
        sendItemFactorsUpdate(itemIid, vertex.getId(), iDelta);
        sendItemFactorsUpdate(itemJid, vertex.getId(), jDelta);
    }

    /**
     * Compute the scalar product of a matrix row with the difference vector of two other matrix rows.
     * Port from mymedialite.
     * for (int k=1; k<u.size(); k++){//we skip the index 0 as it is reserved for item biases.
     *     res += u.get(k).get() * (i.get(k).get() - j.get(k).get());
     * }
     * @param u
     * @param i
     * @param j
     * @return
     */
    private float rowScalarProductWithRowDifference(FloatMatrix u, FloatMatrix i, FloatMatrix j) {
        FloatMatrix ret = u.mul(i.sub(j));
        ret.put(0, 0);
        return ret.sum();
    }

    public static <T> ArrayList<T> copyIterator(Iterable<T> iter) {
        Iterator<T> it = iter.iterator();
        ArrayList<T> copy = new ArrayList<T>();
        while (it.hasNext())
            copy.add(it.next());
        return copy;
    }

	@Override
	/**
	 * BPR samples single relevant and single irrelevant item.
	 */
	int getBufferSize() {
		return 1;
	}
}