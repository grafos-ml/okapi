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
import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.jblas.FloatMatrix;


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
//FIXME implement gapFM (maybe only sampling logic first) instead of TFMAP
public class TFMAPRankingComputation extends AbstractCFRankingComputation{

    protected static final Logger logger = Logger.getLogger(TFMAPRankingComputation.class);

    private int bufferSize;//buffer size (irelevant + relevant)

    public void computeModelUpdates(
            Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex,
            Iterable<FloatMatrixMessage> buffer) {
        if (vertex.getId().isUser()){
            //each user should receive exactly 2K messages, where K is the number of relevant items.

            //1. I will update the user factors using the relevant items.
            FloatMatrix updateUser = updateUser(buffer, vertex);
            applyUpdate(updateUser.mul(learnRate), vertex);

            //2. I will compute updates for the items and send the updates to the items to update themselves.
            for (FloatMatrixMessage msg : buffer) {
                FloatMatrix update = updateItem(msg.getFactors(), vertex.getValue(), buffer);
                sendItemFactorsUpdate(msg.getSenderId(), vertex.getId(), update);
            }
        }
    }

    private FloatMatrix updateItem(FloatMatrix V_i,
                                               FloatMatrix U_m,
                                               Iterable<FloatMatrixMessage> buffer) {

        int y_mi = 0; //replacement for sum{y_{mi}}
        float f_mi = U_m.dot(V_i);
        float buffer_sum = 0;
        for (FloatMatrixMessage msgJ : buffer) {
            if (isRelevant(msgJ)){
                y_mi++;
                FloatMatrix V_j = msgJ.getFactors();
                buffer_sum += logfd(f_mi) * logf(dot_of_difference(U_m, V_j, V_i)) +
                        ( logf(U_m.dot(V_j)) - logf(f_mi) * logfd(dot_of_difference(U_m, V_j, V_i)));
            }
        }
        FloatMatrix newV_i = U_m.mul(buffer_sum/y_mi).add(V_i.mul(-reg));
        return newV_i;
    }

    /**
     *
     *
     * @param buffer contains relevant and irrelevant items.
     * @param vertex
     * @return
     */
    private FloatMatrix updateUser(Iterable<FloatMatrixMessage> buffer,
                                               Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex) {
        int y_mi = 0; //the counter \sum_i^M{y_{mi}}
        FloatMatrix U_m = vertex.getValue();//current U_m factors
        FloatMatrix newU = FloatMatrix.zeros(U_m.columns);//the new U_m factor
        for (FloatMatrixMessage msgI : buffer) {//first sum;
            if (isRelevant(msgI)){//replacement of y_{mi}
                y_mi++;
                FloatMatrix V_i = msgI.getFactors();
                FloatMatrix deltaV_i = V_i.mul(computeDelta(U_m, V_i, buffer));//[\delta \elementwise V_i
                float gf_mi = logf(U_m.dot(V_i)); //g(f_{mi})
                FloatMatrix V_sum = FloatMatrix.zeros(V_i.columns); //where we store the second sum which runs over j
                for (FloatMatrixMessage msgJ : buffer) {
                    if (isRelevant(msgJ)){//replacement of y_{mj}
                        FloatMatrix V_j = msgJ.getFactors();
                        V_sum = V_sum.add(V_j.mul(logfd( dot_of_difference(U_m, V_j, V_i)) ));
                    }
                }
                newU = deltaV_i.add(V_sum.add(gf_mi));
            }
        }
        return newU.mul(1.0f/y_mi).add(U_m.mul(-reg));
    }

    /**
     * Substitution for delta abotve eq.8.
     * @param v_i
     * @param messages
     * @return
     */
    private float computeDelta(FloatMatrix m_i, FloatMatrix v_i, Iterable<FloatMatrixMessage> messages) {
        float f_mi = m_i.dot(v_i);
        float first = 0;
        float second = 0;
        for (FloatMatrixMessage msgJ : messages) {
            if(isRelevant(msgJ)){
                FloatMatrix v_j = msgJ.getFactors();
                first += logf(dot_of_difference(m_i, v_j, v_i));
                second += logfd(dot_of_difference(m_i, v_j, v_i));
            }
        }
        return logfd(f_mi) * first + logf(f_mi) * second;
    }

    private double dot_of_difference(FloatMatrix m_i, FloatMatrix v_j, FloatMatrix v_i) {
        return m_i.dot(v_j) - m_i.dot(v_i);
    }

    @Override
    int getBufferSize() {
        return bufferSize;
    }

}
