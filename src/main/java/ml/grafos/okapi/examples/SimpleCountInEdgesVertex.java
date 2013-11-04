package ml.grafos.okapi.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
  name = "Count input edges",
  description = "Counts input edges for each vertex")
public class SimpleCountInEdgesVertex extends
  BasicComputation<LongWritable, DoubleWritable, FloatWritable, 
  DoubleWritable> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(SimpleCountInEdgesVertex.class);

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) {
    /** Initialize vertex value to zero */
    if (getSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(0d));
    }
    /** Initialize counter */
    double count = 0d;
    /** Count messages */
    for (@SuppressWarnings("unused")
    DoubleWritable message : messages) {
      count += 1d;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + 
          " got a message. New total = " + count);
    }
    /** Save count into vertex value */
    vertex.setValue(new DoubleWritable(count));

    /** Send to all neighbors a message */
    for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Vertex " + vertex.getId() + " sent a message to " +
          edge.getTargetVertexId());
      }
      if (getSuperstep() < 2) {
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(1d));
      }
    }
    vertex.voteToHalt();
  }
}
