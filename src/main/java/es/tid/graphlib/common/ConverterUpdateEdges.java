package es.tid.graphlib.common;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

import es.tid.graphlib.sybilrank.SybilRank;
import es.tid.graphlib.sybilrank.SybilRank.EdgeValue;

public class ConverterUpdateEdges<I extends WritableComparable, 
  V extends Writable, M2 extends Writable>
  extends AbstractComputation<I, V, EdgeValue, I, M2> {
  
  private byte edgeWeight;

  @Override
  public void compute(Vertex<I, V, EdgeValue> vertex,
      Iterable<I> messages) throws IOException {
    for (I other : messages) {
      EdgeValue edgeValue = vertex.getEdgeValue(other);
      if (edgeValue == null) {
        edgeValue = new EdgeValue();
        edgeValue.setWeight((byte) 1);
        I clonedId;
        try {
          clonedId = (I)other.getClass().newInstance();
        } catch (InstantiationException e) {
          throw new IOException(e);
        } catch (IllegalAccessException e) {
          throw new IOException(e);
        }
        ReflectionUtils.copy(getContext().getConfiguration(), other, clonedId);
        Edge<I, EdgeValue> edge = EdgeFactory.create(clonedId, edgeValue);
        vertex.addEdge(edge);
      } else {
        edgeValue = new EdgeValue();
        edgeValue.setWeight(edgeWeight);
        vertex.setEdgeValue(other, edgeValue);
      }
    }
  }

  @Override
  public void preSuperstep() {
    edgeWeight = (byte) getContext().getConfiguration().getInt(
        SybilRank.EDGE_WEIGHT, SybilRank.DEFAULT_EDGE_WEIGHT);
  }
}