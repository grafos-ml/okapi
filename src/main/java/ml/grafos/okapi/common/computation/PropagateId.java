/**
 * Copyright 2013 Grafos.ml
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

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This computation is used simply to propagate the ID of a vertex to all its
 * neighbors. 
 * 
 * @author dl
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M1>
 */
public class PropagateId<I extends WritableComparable, 
  V extends Writable, E extends Writable, M1 extends Writable> 
  extends AbstractComputation<I,V,E,M1,I> {

  @Override
  public void compute(Vertex<I,V,E> vertex, 
      Iterable<M1> messages) throws IOException {
    sendMessageToAllEdges(vertex, vertex.getId());
  }
}