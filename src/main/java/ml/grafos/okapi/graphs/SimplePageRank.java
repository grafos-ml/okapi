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
package ml.grafos.okapi.graphs;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Basic Pregel PageRank implementation.
 *
 * This version initializes the value of every vertex to 1/N, where N is the
 * total number of vertices.
 *
 * The maximum number of supersteps is configurable.
 */
public class SimplePageRank extends BasicComputation<LongWritable,
  DoubleWritable, FloatWritable, DoubleWritable> {
  /** Default number of supersteps */
  public static final int MAX_SUPERSTEPS_DEFAULT = 30;
  /** Property name for number of supersteps */
  public static final String MAX_SUPERSTEPS = "pagerank.max.supersteps";

  /** Logger */
  private static final Logger LOG =
    Logger.getLogger(SimplePageRank.class);

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(1f / getTotalNumVertices()));
    }
    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue =
        new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
      vertex.setValue(vertexValue);
    }

    if (getSuperstep() < getContext().getConfiguration().getInt(
      MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT)) {

      long edges = vertex.getNumEdges();
      sendMessageToAllEdges(vertex,
          new DoubleWritable(vertex.getValue().get() / edges));
    } else {
      vertex.voteToHalt();
    }
  }
}
