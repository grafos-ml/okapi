package ml.grafos.okapi.graphs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.giraph.graph.VertexResolver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * <p>
 * The <i>k-core</i> of a graph is the subgraph in which all vertices have
 * degree of at least <i>k</i>.
 * </p>
 * <p>
 * We find the k-core of a graph by iteratively removing vertices with degree
 * less than <i>k</i>.
 * </p>
 * <p>
 * The algorithms stops when there are no more vertex removals. At the end of
 * the execution, the remaining graph represents the k-core. It is possible that
 * the result in an empty graph.
 * </p>
 * <p>
 * http://en.wikipedia.org/wiki/Degeneracy_(graph_theory)
 * </p>
 * @author dl
 *
 */
public class KCore {
  
  /**
   * K-value property.
   */
  public static String K_VALUE = "core.k";

  /**
   * Default K-value.
   */
  public static int K_VALUE_DEFAULT = -1;

  public static class KCoreComputation extends BasicComputation<LongWritable, 
  NullWritable, NullWritable, LongWritable>{

    @Override
    public void compute(
        Vertex<LongWritable, NullWritable, NullWritable> vertex,
        Iterable<LongWritable> messages) throws IOException {
      
      HashSet<LongWritable> toDelete = new HashSet<LongWritable>();

      for (LongWritable id : messages) {
        toDelete.add(new LongWritable(id.get()));
      }

      Iterator<MutableEdge<LongWritable,NullWritable>> edgeIterator = 
          vertex.getMutableEdges().iterator();
      
      while (edgeIterator.hasNext()) {
        if (toDelete.contains(edgeIterator.next().getTargetVertexId())) {
          edgeIterator.remove();
        }
      }
      
      if (vertex.getNumEdges()<getConf().getInt(K_VALUE, K_VALUE_DEFAULT)) {
        sendMessageToAllEdges(vertex, vertex.getId());
        removeVertexRequest(vertex.getId());
      }
      
      vertex.voteToHalt();
    }
  }
  
  /**
   * The only desired mutation in this application is the deletion of vertices.
   * We use this vertex resolver to ensure that a vertex that gets deleted will
   * not be created again if a message is sent to it.
   */
  public static class KCoreVertexResolver 
  implements VertexResolver<LongWritable, NullWritable, NullWritable> {

    @Override
    public Vertex resolve(LongWritable vertexId, Vertex vertex,
        VertexChanges vertexChanges, boolean hasMessages) {
      return null;
    }
    
  }
}
