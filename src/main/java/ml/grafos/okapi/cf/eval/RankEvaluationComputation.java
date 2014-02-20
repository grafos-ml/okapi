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
package ml.grafos.okapi.cf.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import ml.grafos.okapi.aggregators.FloatAvgAggregator;
import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;


/**
 * Computes a rank measure using giraph infrastructure;
 * As a message we use DoubleArrayListWritable.
 * @author linas
 *
 */
public class RankEvaluationComputation extends BasicComputation<CfLongId, 
FloatMatrixWritable, BooleanWritable, FloatMatrixMessage>{

  private static int numberSamples;
  private static long minItemId;
  private static long maxItemId;
  private static final FloatMatrixWritable emptyList = new FloatMatrixWritable(0);
  private static int k;

  private static final String AVG_PRECISION_AGGREGATOR = "precision.avg";

  @Override
  public void compute(
      Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex,
      Iterable<FloatMatrixMessage> messages) throws IOException {

    minItemId = Integer.parseInt(getConf().get("minItemId"));
    maxItemId = Integer.parseInt(getConf().get("maxItemId"));

    numberSamples = Integer.parseInt(getConf().get("numberSamples", "100"));
    k = Integer.parseInt(getConf().get("k", "5"));
    if (getSuperstep() == 0){
      sampleIrrelevantEdges(vertex);
      sendUserDataToItems(vertex);
    }else if(getSuperstep() == 1){
      computeScoreAndSendBack(vertex, messages);
    }else if(getSuperstep() == 2){
      computeRankingMeasure(vertex, messages);
    }
    vertex.voteToHalt();
  }

  public void computeRankingMeasure(
      Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex,
      Iterable<FloatMatrixMessage> messages) {
    if (vertex.getId().isUser()){
      ArrayList<FloatBoolean> scores = new ArrayList<FloatBoolean>(); 
      for(FloatMatrixMessage msg : messages){
        scores.add(new FloatBoolean(msg.getScore(), 
            vertex.getEdgeValue(msg.getSenderId()).get()));
      }
      Collections.sort(scores);
      float rankingMeasure = computeRecall(scores, this.k);

      aggregate(AVG_PRECISION_AGGREGATOR, 
          new FloatAvgAggregator.PartialAvg(rankingMeasure,1));
    }
  }

  private float computeRecall(ArrayList<FloatBoolean> scores, int k) {
    if (null == scores || scores.size() == 0)
      return 0;
    int cnt = 0;
    float relevant = 0;
    for (FloatBoolean doubleBoolean : scores) {
      if (doubleBoolean.isRelevant())
        relevant += 1;
      cnt += 1;
      if (cnt >= k)
        break;
    }
    return relevant/cnt;
  }

  public void computeScoreAndSendBack(
      Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex,
      Iterable<FloatMatrixMessage> messages) {
    if (vertex.getId().isItem()){//only items send messages
      for(FloatMatrixMessage msg : messages){
        float score = msg.getFactors().dot(vertex.getValue());
        FloatMatrixMessage msgToSendBack = 
            new FloatMatrixMessage(vertex.getId(), emptyList, score);
        sendMessage(msg.getSenderId(), msgToSendBack);
      }
    }
  }

  public void sendUserDataToItems(
      Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex) {
    if (vertex.getId().isUser()){
      FloatMatrixMessage msg = new FloatMatrixMessage(
          vertex.getId(), vertex.getValue(), -1.0f);
      sendMessageToAllEdges(vertex, msg);
    }
  }

  public void sampleIrrelevantEdges(
      Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex) {
    if (vertex.getId().isUser()){//only users
      Iterable<Edge<CfLongId, BooleanWritable>> edges = vertex.getEdges();
      HashSet<CfLongId> relevant = new HashSet<CfLongId>();
      for (Edge<CfLongId, BooleanWritable> e : edges) {
        relevant.add(e.getTargetVertexId());
      }
      for (int i=0; i<numberSamples; i++){
        CfLongId random = getRandomItemId(relevant);
        vertex.addEdge(EdgeFactory.create(random, new BooleanWritable(false)));
      }
    }
  }

  protected CfLongId getRandomItemId(HashSet<CfLongId> relevant) {
    Random r = new Random();
    int top = (int)(maxItemId-minItemId)+1;
    int i = r.nextInt(top)+(int)minItemId;
    CfLongId randId = new CfLongId((byte)1, i);
    int maxCnt = 0;
    while (relevant.contains(randId)){
      i = r.nextInt((int)maxItemId-(int)minItemId)+(int)minItemId;
      if (maxCnt > 1000000){//just to prevent an infinity loop
        throw new RuntimeException("Can not sample a new irrelevant item");
      }
      maxCnt += 1;
      randId = new CfLongId((byte)1, i);
    }
    return randId;
  }

  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute extends DefaultMasterCompute {

    @Override
    public final void initialize() throws InstantiationException,
    IllegalAccessException {
      registerAggregator(AVG_PRECISION_AGGREGATOR, FloatAvgAggregator.class);
    }
  }
}
