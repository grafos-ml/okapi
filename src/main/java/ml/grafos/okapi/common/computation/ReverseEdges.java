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
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;


public class ReverseEdges<I extends WritableComparable, 
  V extends Writable, E extends Writable, M1 extends WritableComparable, 
  M2 extends Writable> 
  extends AbstractComputation<I, V, E, MessageWrapper<I, M1>, M2> {
  
  @Override
  public void compute(Vertex<I, V, E> vertex,
      Iterable<MessageWrapper<I,M1>> messages) throws IOException {
    for (MessageWrapper<I,M1> msg : messages) {
      E edgeValue = vertex.getEdgeValue(msg.getSourceId());
      
      if (edgeValue == null) {
        I clonedId = null;
        E clonedEdgeValue = null;
        
        try {
          clonedId = (I)msg.getSourceId().getClass().newInstance();
          clonedEdgeValue = (E)msg.getMessage().getClass().newInstance();
        } catch (InstantiationException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new IOException(e);
        }
        
        ReflectionUtils.copy(
            getContext().getConfiguration(), msg.getSourceId(), clonedId);
        ReflectionUtils.copy(
            getContext().getConfiguration(), msg.getMessage(), clonedEdgeValue);
        
        Edge<I, E> edge = EdgeFactory.create(clonedId, clonedEdgeValue);
        vertex.addEdge(edge);
      } 
    }
  }
}