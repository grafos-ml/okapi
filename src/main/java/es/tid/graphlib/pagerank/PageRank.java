
package es.tid.graphlib.pagerank;

import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
  name = "Page rank - modified")

public class PageRank extends Vertex<LongWritable,
  DoubleWritable, FloatWritable, DoubleWritable> {
  /** Default number of supersteps */
  public static final int MAX_SUPERSTEPS_DEFAULT = 30;
  /** Property name for number of supersteps */
  public static final String MAX_SUPERSTEPS = "pagerank.max.supersteps";
  /** Decimals */
  public static final int DECIMALS = 4;
  /** Tolerance */
  public static final double TOLERANCE = 0.003;
  /** Initial value to be used for the L2Norm case */
  private DoubleWritable initialValue = new DoubleWritable();
  /** L2Norm Error */
  private double l2normError = 0d;

  /**
   * Compute method
   * @param messages Messages received
   */
  @Override
  public void compute(Iterable<DoubleWritable> messages) {
    /** Flag for checking if parameter for L2Norm is enabled */
    boolean l2normFlag = getContext().getConfiguration().getBoolean(
      "PageRank.l2norm", false);

    if (getSuperstep() == 0) {
      if (l2normFlag) {
        initialValue = getValue();
        // System.out.println("**S: " + getSuperstep() +
        // ", vertex, init_value: " + getId() + ", " + initialValue);
      }
      /** Set value with X decimals */
      keepXdecimals(new DoubleWritable(1d / getTotalNumVertices()), DECIMALS);
    }

    if (getSuperstep() >= 1) {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      DoubleWritable vertexValue =
        new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
      setValue(vertexValue);

      /** Compute L2Norm */
      if (l2normFlag) {
        l2normError = getL2Norm(initialValue, getValue());
        /* System.out.println("**S: " + getSuperstep() + ", VertexId: " +
          getId() + ", [" + initialValue + ", " + getValue() + "], " +
            l2normError); */
      }
    }

    if (l2normFlag) {
      if (getSuperstep() == 0 || (l2normError > TOLERANCE &&
          getSuperstep() < getContext().getConfiguration().getInt(
              MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT))) {
        sendMessageToAllEdges(new DoubleWritable(getValue().get() /
          getNumEdges()));
      } else {
        voteToHalt();
      }
    } else {
      if (getSuperstep() < getContext().getConfiguration().getInt(
          MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT)) {
        sendMessageToAllEdges(new DoubleWritable(getValue().get() /
          getNumEdges()));
      }
    }
  } // EoF compute()

  /**
   * Decimal Precision of latent vector values
   *
   * @param value Value to be truncated
   * @param x Number of decimals to be kept
   */
  public void keepXdecimals(DoubleWritable value, int x) {
    double num = 1;
    for (int i = 0; i < x; i++) {
      num *= 10;
    }
    setValue(new DoubleWritable(
      (double) (Math.round(getValue().get() * num) / num)));
  }

  /**
   * Calculate the L2Norm on the initial and final value of vertex
   *
   * @param valOld Old vertex value
   * @param valNew New vertex value
   * @return result of L2Norm equation
   * */
  public double getL2Norm(DoubleWritable valOld, DoubleWritable valNew) {
    return Math.sqrt(Math.pow(valOld.get() - valNew.get(), 2));
  }

}
