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
package ml.grafos.okapi.common.computation;

import java.io.IOException;

import ml.grafos.okapi.common.data.MessageWrapper;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Many applications require a vertex to send a list of its friends to all
 * its neighbors in the graph. This is common in algorithms that require a
 * vertex to know its 2-hop neighborhood, such as triangle counting or computing
 * the clustering coefficient.
 * 
 * Such algorithms can re-use this computation class. They only need to extend
 * it and specify the parameter types.
 * 
 * @author dl
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M1>
 */
public abstract class SendFriends<I extends WritableComparable, 
  V extends Writable, E extends Writable, M extends MessageWrapper<
  ? extends WritableComparable, ? extends ArrayListWritable>> 
  extends BasicComputation<I,V,E,M> {

  @Override
  public void compute(Vertex<I,V,E> vertex, Iterable<M> messages) 
      throws IOException {

    final Vertex<I,V,E> _vertex = vertex;

    final ArrayListWritable friends =  new ArrayListWritable() {
      @Override
      public void setClass() {
        setClass(_vertex.getId().getClass());
      }
    };

    for (Edge<I,E> edge : vertex.getEdges()) {
      friends.add(WritableUtils.clone(edge.getTargetVertexId(), getConf()));
    }

    MessageWrapper<I, ArrayListWritable<I>> msg = 
        new MessageWrapper<I, ArrayListWritable<I>>() {

      @Override
      public Class getVertexIdClass() {
        return _vertex.getClass();
      }

      @Override
      public Class getMessageClass() {
        return friends.getClass();
      }
    };
    
    msg.setSourceId(vertex.getId());
    msg.setMessage(friends);
    sendMessageToAllEdges(vertex, (M)msg);
  }
}