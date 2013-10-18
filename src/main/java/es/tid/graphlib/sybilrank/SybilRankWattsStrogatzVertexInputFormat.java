package es.tid.graphlib.sybilrank;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.PseudoRandomUtils;
import org.apache.giraph.io.formats.WattsStrogatzVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.tid.graphlib.sybilrank.SybilRank.VertexValue;

/**
 * This class generates random graphs according to the Watts-Strogats model. 
 * Use this class to try the algorithm with random graphs.
 * 
 * This class replicates almost all the code of the 
 * {@link WattsStrogatzVertexInputFormat} and also adds a random labeling of the
 * vertices as trusted or not, which is necessary for the SybilRank algorithm.
 * 
 * The wattsStrogatz.sybilrank.trust.probability property defines the 
 * probability that a vertex is labeled as trusted.
 * 
 */
public class SybilRankWattsStrogatzVertexInputFormat extends
  VertexInputFormat<LongWritable, VertexValue, DoubleWritable> {
  /** The number of vertices in the graph */
  private static final String AGGREGATE_VERTICES =
      "wattsStrogatz.aggregateVertices";
  /** The number of outgoing edges per vertex */
  private static final String EDGES_PER_VERTEX =
      "wattsStrogatz.edgesPerVertex";
  /** The probability to re-wire an outgoing edge from the ring lattice */
  private static final String BETA =
      "wattsStrogatz.beta";
  /** The seed to generate random values for pseudo-randomness */
  private static final String SEED =
      "wattsStrogatz.seed";
  /** Probability that a vertex is labeled as trusted */
  private static final String TRUST_PROBABILITY = 
      "wattsStrogatz.sybilrank.trust.probability";
  /** Default value for the trust probability */
  private static final float TRUST_PROBABILITY_DEFAULT = 0.2f;

  @Override
  public void checkInputSpecs(Configuration conf) { }

  @Override
  public final List<InputSplit> getSplits(final JobContext context,
      final int minSplitCountHint) throws IOException, InterruptedException {
    return PseudoRandomUtils.getSplits(minSplitCountHint);
  }

  @Override
  public VertexReader<LongWritable, VertexValue, DoubleWritable>
  createVertexReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new WattsStrogatzVertexReader();
  }

  /**
   * Vertex reader used to generate the graph
   */
  private static class WattsStrogatzVertexReader extends
    VertexReader<LongWritable, VertexValue, DoubleWritable> {
    /** the re-wiring probability */
    private float beta = 0;
    /** The total number of vertices */
    private long aggregateVertices = 0;
    /** The starting vertex id for this split */
    private long startingVertexId = -1;
    /** The number of vertices read so far */
    private long verticesRead = 0;
    /** The total number of vertices in the split */
    private long totalSplitVertices = -1;
    /** the total number of outgoing edges per vertex */
    private int edgesPerVertex = -1;
    /** The target ids of the outgoing edges */
    private final LongSet destVertices = new LongOpenHashSet();
    /** The random values generator */
    private Random rnd;
    /** The reusable edge */
    private ReusableEdge<LongWritable, DoubleWritable> reusableEdge = null;
    /** Trust probability */
    private float trustProb = TRUST_PROBABILITY_DEFAULT;

    /**
     * Default constructor
     */
    public WattsStrogatzVertexReader() { }

    @Override
    public void initialize(InputSplit inputSplit,
        TaskAttemptContext context) throws IOException {
      trustProb = getConf().getFloat(
          TRUST_PROBABILITY, TRUST_PROBABILITY_DEFAULT);
      beta = getConf().getFloat(
          BETA, 0.0f);
      aggregateVertices = getConf().getLong(
          AGGREGATE_VERTICES, 0);
      BspInputSplit bspInputSplit = (BspInputSplit) inputSplit;
      long extraVertices = aggregateVertices % bspInputSplit.getNumSplits();
      totalSplitVertices = aggregateVertices / bspInputSplit.getNumSplits();
      if (bspInputSplit.getSplitIndex() < extraVertices) {
        ++totalSplitVertices;
      }
      startingVertexId = bspInputSplit.getSplitIndex() *
          (aggregateVertices / bspInputSplit.getNumSplits()) +
          Math.min(bspInputSplit.getSplitIndex(), extraVertices);
      edgesPerVertex = getConf().getInt(
          EDGES_PER_VERTEX, 0);
      if (getConf().reuseEdgeObjects()) {
        reusableEdge = getConf().createReusableEdge();
      }
      int seed = getConf().getInt(SEED, -1);
      if (seed != -1) {
        rnd = new Random(seed);
      } else {
        rnd = new Random();
      }
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return totalSplitVertices > verticesRead;
    }

    /**
     * Return a long value uniformly distributed between 0 (inclusive) and n.
     *
     * @param n the upper bound for the random long value
     * @return the random value
     */
    private long nextLong(long n) {
      long bits;
      long val;
      do {
        bits = (rnd.nextLong() << 1) >>> 1;
        val = bits % n;
      } while (bits - val + (n - 1) < 0L);
      return val;
    }

    /**
     * Get a destination id that is not already in the neighborhood and
     * that is not the vertex itself (no self-loops). For the second condition
     * it expects destVertices to contain the own id already.
     *
     * @return the destination vertex id
     */
    private long getRandomDestination() {
      long randomId;
      do {
        randomId = nextLong(aggregateVertices);
      } while (!destVertices.add(randomId));
      return randomId;
    }

    @Override
    public Vertex<LongWritable, VertexValue, DoubleWritable>
    getCurrentVertex() throws IOException, InterruptedException {
      Vertex<LongWritable, VertexValue, DoubleWritable> vertex =
          getConf().createVertex();
      long vertexId = startingVertexId + verticesRead;
      OutEdges<LongWritable, DoubleWritable> edges =
          getConf().createOutEdges();
      edges.initialize(edgesPerVertex);
      destVertices.clear();
      destVertices.add(vertexId);
      long destVertexId = vertexId - edgesPerVertex / 2;
      if (destVertexId < 0) {
        destVertexId = aggregateVertices + destVertexId;
      }
      for (int i = 0; i < edgesPerVertex + 1; ++i) {
        if (destVertexId != vertexId) {
          Edge<LongWritable, DoubleWritable> edge =
              (reusableEdge == null) ? getConf().createEdge() : reusableEdge;
          edge.getTargetVertexId().set(
              rnd.nextFloat() < beta ? getRandomDestination() : destVertexId);
          edge.getValue().set(rnd.nextDouble());
          edges.add(edge);
        }
        destVertexId = (destVertexId + 1) % aggregateVertices;
      }
      vertex.initialize(new LongWritable(vertexId),
          new VertexValue(rnd.nextDouble(), rnd.nextFloat()<trustProb), edges);
      ++verticesRead;
      return vertex;
    }

    @Override
    public void close() throws IOException { }

    @Override
    public float getProgress() throws IOException {
      return verticesRead * 100.0f / totalSplitVertices;
    }
  }
}
