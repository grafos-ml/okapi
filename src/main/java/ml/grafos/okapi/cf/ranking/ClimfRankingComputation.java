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

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.jblas.FloatMatrix;


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
            Vertex<CfLongId,FloatMatrixWritable,FloatWritable> vertex,
            Iterable<FloatMatrixMessage> messages) {
        //now do the magic computation with relevant and send the updates to items.
        if (vertex.getId().isUser()) // only for users
            updateModel(vertex.getValue(), messages, vertex);
    }


    /**
     * Updates the model based on the factors received.
     * @param u   Model parameters for user u
     * @param messages
     * @param vertex  Current user vertex
     */
    private void updateModel(FloatMatrix u,
                             Iterable<FloatMatrixMessage> messages,
                             Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {
        //Compute User update
        double tempdiff;
        FloatMatrix uDelta = FloatMatrix.zeros(u.columns);
        FloatMatrix partialOneDelta;
        FloatMatrix partialTwoDelta = FloatMatrix.zeros(u.columns);

        for (FloatMatrixMessage msg : messages) {
            FloatMatrix V_j = msg.getFactors();
            float fij = u.dot(V_j);
            partialOneDelta = V_j.mul(logf(-1.0f*fij));
            for (FloatMatrixMessage msginner : messages) {
                FloatMatrix V_k = msginner.getFactors();
                tempdiff = fij - u.dot(V_k);
                partialTwoDelta = V_j.sub(V_k).mul(logfd(tempdiff)/(1-logf(tempdiff)));
            }
            uDelta.addi(partialOneDelta);
            uDelta.addi(partialTwoDelta);

        }
        uDelta.sub(u.mul(reg));
        uDelta.mul(learnRate);

        //Compute Item Updates
        for (FloatMatrixMessage msg : messages) {
            FloatMatrix vDelta = FloatMatrix.zeros(u.columns);
            FloatMatrix V_j = msg.getFactors();
            CfLongId Itemid = msg.getSenderId();
            float fij = u.dot(V_j);
            float partialSumOne = logf(-1.0*fij);
            float partialSumTwo = 0;
            for (FloatMatrixMessage msginner : messages) {
                FloatMatrix V_k = msginner.getFactors();
                tempdiff = fij - u.dot(V_k);
                partialSumTwo += logfd(tempdiff)*(1.0/(1.0-logf(-1.0*tempdiff)) - 1.0/(1.0 - logf(tempdiff) ) );
            }

            vDelta.add(u.mul(partialSumTwo + partialSumOne));
            vDelta.add(V_j.mul(reg));
            vDelta.mul(learnRate);

            sendItemFactorsUpdate(Itemid, vertex.getId(), vDelta);
        }

        //do the user update
        applyUpdate(uDelta, vertex);
    }

    @Override
    int getBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }
}

