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

import java.io.IOException;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Random;

import ml.grafos.okapi.common.Parameters;
import ml.grafos.okapi.common.data.MapWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * <p>
 * This algorithm takes as input a weighted graph and computes the shortest 
 * distances from a selected set of source vertices to all other vertices in the
 * graph. 
 * </p> 
 * <p>
 * This can be used, for instance, to compute distances from a set of "landmark"
 * vertices in a graph. The distances from the landmarks may then be used to 
 * compute other metrics such as approximate all-pair shortest distances and
 * diameter estimation.
 * <p> 
 * Although this can be done by running the single-source shortest paths
 * algorithm multiple times, this algorithm is more efficient.
 * </p> 
 * <p> 
 * Every vertex maintains a map with current shortest distances from each
 * source. Initially every source starts propagating their distances to their
 * neighbors. After that, if the distance from one of the sources changes, a 
 * vertex propagates only the change with respect to that source.
 * </p> 
 * <p>
 * The execution finishes when no changes occur. At the end every vertex's
 * value holds the distance from each source. If a vertex does not contain a 
 * specific source ID, then its distance to the specific source is considered to
 * be infinite.
 * </p> 
 * <p>
 * Note that the larger the number of sources selected, the largest the
 * computational overhead, memory footprint and network traffic incurred by this 
 * algorithm, so be cautious about how many sources you select.
 * </p> 
 * <p>
 * The user can either define a fraction of vertices to be selected randomly 
 * as sources or can explicitly define list of source IDs separated by a ':'. If
 * non are specified, vertex with ID=1 will be selected as the single source.
 * </p>
 */
@Algorithm(
    name = "Multi-source shortest paths",
    description = "Finds all shortest paths from a set of selected vertices"
    )
public class MultipleSourceShortestPaths {

  /**
   * Fraction of vertices to select as sources.
   */
  public static final String SOURCES_FRACTION = "sources.fraction";

  /**
   * Default value for fraction of vertices to be selected as sources.
   */
  public static final float SOURCES_FRACTION_DEFAULT = -1f;

  /**
   * List of vertex ids to select as sources.
   */
  public static final String SOURCES_LIST = "sources.list";

  /**
   * Default list of vertices to select as sources.
   */
  public static final String SOURCES_LIST_DEFAULT = "1"; 
  
  private static final Pattern SEPARATOR = Pattern.compile("[:]");
  private static final Text IS_SOURCE_KEY = new Text("is.source");
  private static final BooleanWritable TRUE = new BooleanWritable(true);

  /**
   * Returns true if this vertex is labeled as one of the sources.
   */
  private static boolean isSource(
      Vertex<LongWritable, MapWritable, FloatWritable> vertex) {
    return vertex.getValue().get(IS_SOURCE_KEY)!=null;
  }

  /**
   * Initializes the value of the source vertices.
   * @author dl
   *
   */
  public static class InitSources extends BasicComputation<LongWritable, 
  MapWritable, FloatWritable, MapWritable>{

    @Override
    public void compute(
        Vertex<LongWritable, MapWritable, FloatWritable> vertex,
        Iterable<MapWritable> messages) throws IOException {

      MapWritable value = new MapWritable();
      vertex.setValue(value);

      float fraction = getConf().getFloat(
          SOURCES_FRACTION, SOURCES_FRACTION_DEFAULT);
      if (fraction>0) {
        Random generator;
        if (Parameters.RANDOM_SEED.get(getConf())>0) {
          generator = new Random(Parameters.RANDOM_SEED.get(getConf()));
        } else {
          generator = new Random();
        }
        if (generator.nextFloat()<fraction) {
          value.put(IS_SOURCE_KEY, TRUE);
        }
      } else {
        String[] sources = SEPARATOR.split(
            getConf().get(SOURCES_LIST, SOURCES_LIST_DEFAULT));
        for (String src : sources) {
          if (vertex.getId().get() == Long.parseLong(src)) {
            // This vertex is in the source list
            value.put(IS_SOURCE_KEY, TRUE);
            break;
          }
        }
      }

      // If this is a source vertex, propagate distances.
      if (isSource(vertex)) {
        value.put(vertex.getId(), new FloatWritable(0));
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
          MapWritable distanceUpdates = new MapWritable();
          distanceUpdates.put(vertex.getId(), edge.getValue());
          sendMessage(edge.getTargetVertexId(), distanceUpdates);
        }
      }

      vertex.voteToHalt();
    }
  }

  /**
   * Implements the main logic of the multi-source shortest paths computation.
   * @author dl
   *
   */
  public static class MultiSourceShortestPathsComputation 
  extends BasicComputation<LongWritable, MapWritable, FloatWritable, 
  MapWritable> {

    @Override
    public void compute(
        Vertex<LongWritable, MapWritable, FloatWritable> vertex,
        Iterable<MapWritable> messages) {

      MapWritable distanceMap = vertex.getValue();
      MapWritable changedDistances = new MapWritable();

      for (MapWritable msg : messages) {
        for (Entry entry : msg.entrySet()) {
          LongWritable src = (LongWritable)entry.getKey();
          FloatWritable distance = (FloatWritable)entry.getValue();

          FloatWritable currentDistance = (FloatWritable)distanceMap.get(src);

          if (currentDistance==null) {
            // First time we see this source id
            currentDistance = new FloatWritable(Float.MAX_VALUE);
            distanceMap.put(new LongWritable(src.get()), currentDistance);
          }

          if (distance.get()<currentDistance.get()) {
            currentDistance.set(distance.get());
            changedDistances.put(src, new FloatWritable(distance.get()));
          }
        }
      }

      if (changedDistances.size()>0) {
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
          MapWritable distanceUpdates = new MapWritable();
          for (Entry entry : changedDistances.entrySet()) {
            float newDistance = 
                ((FloatWritable)entry.getValue()).get() + edge.getValue().get();
            distanceUpdates.put(
                (LongWritable)entry.getKey(), new FloatWritable(newDistance));
          }
          sendMessage(edge.getTargetVertexId(), distanceUpdates);
        } 
      }

      vertex.voteToHalt();
    }
  }

  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute extends DefaultMasterCompute {

    @Override
    public final void compute() {
      long superstep = getSuperstep();
      if (superstep == 0) {
        setComputation(InitSources.class);
      } else {
        setComputation(MultiSourceShortestPathsComputation.class);
      }
    }
  }
}
