/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.tid.graphlib.examples;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A complete edge, the target vertex id, the target vertex value and the edge value.  
 * Can only be one edge with a destination vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public interface EdgeWithVertexValue<I extends WritableComparable, V extends Writable, E extends Writable>
    extends Comparable<EdgeWithVertexValue<I, V, E>> {
  /**
   * Get the target vertex index of this edge
   *
   * @return Target vertex index of this edge
   */
  I getTargetVertexId();

  /**
   * Get the target vertex value of this edge
   *
   * @return Target vertex value of this edge
   */
  V getTargetVertexValue();
  
  /**
   * Get the edge value of the edge
   *
   * @return Edge value of this edge
   */
  E getValue();
}
