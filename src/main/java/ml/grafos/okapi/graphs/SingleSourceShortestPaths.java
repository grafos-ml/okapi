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
package ml.grafos.okapi.graphs;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Implementation of the single-source shortest paths algorithm. It finds the
 * shortest distances from a specified source to all other nodes in the graph.
 * The input graph can be directed or undirected.
 */
@Algorithm(
    name = "Shortest paths",
    description = "Finds all shortest paths from a selected vertex"
)
public class SingleSourceShortestPaths extends BasicComputation<LongWritable, 
DoubleWritable, FloatWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final String SOURCE_ID = "sssp.source.id";
  /** Default shortest paths id */
  public static final long SOURCE_ID_DEFAULT = 1;
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SingleSourceShortestPaths.class);

  /**
   * Is this vertex the source id?
   *
   * @return True if the source id
   */
  private boolean isSource(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) {
    return vertex.getId().get() ==
        getContext().getConfiguration().getLong(SOURCE_ID,
            SOURCE_ID_DEFAULT);
  }

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) {
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }
    
    // In directed graphs, vertices that have no outgoing edges will be created
    // in the 1st superstep as a result of messages sent to them.
    if (getSuperstep() == 1 && vertex.getNumEdges() == 0) {
      vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    }

    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    for (DoubleWritable message : messages) {
      minDist = Math.min(minDist, message.get());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist +
          " vertex value = " + vertex.getValue());
    }
    if (minDist < vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(minDist));
      for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        double distance = minDist + edge.getValue().get();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Vertex " + vertex.getId() + " sent to " +
              edge.getTargetVertexId() + " = " + distance);
        }
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
      }
    }
    vertex.voteToHalt();
  }
}
