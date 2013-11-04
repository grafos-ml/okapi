package es.tid.graphlib.clustering.semiclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.clustering.semiclustering.SemiClustering.SemiClusterTreeSetWritable;

/**
 * Demonstrates the Semi Clustering implementation.
 */
@Algorithm(
  name = "Semi Clustering",
  description = "Put vertices into clusters.")

public class SemiClustering extends BasicComputation<LongWritable,
SemiClusterTreeSetWritable, DoubleWritable, SemiClusterTreeSetWritable> {

  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS_KEYWORD = "semi.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Maximum number of Semi-Clusters. */
  public static final String CLUSTER_LIST_CAPACITY_KEYWORD =
    "semi.clusters.max";
  /** Default value for Maximum Number of Semi-Clusters. */
  public static final int CLUSTER_LIST_CAPACITY_DEFAULT = 2;
  /** Keyword for parameter setting the Maximum No of vertices in a cluster. */
  public static final String CLUSTER_CAPACITY_KEYWORD =
    "semi.cluster.capacity";
  /** Default value for CAPACITY. */
  public static final int CLUSTER_CAPACITY_DEFAULT = 4;
  /** Keyword for parameter setting the Boundary Edge Score Factor. */
  public static final String SCORE_FACTOR_KEYWORD = "semi.score.factor";
  /** Default value for Boundary Edge Score Factor. */
  public static final float SCORE_FACTOR_DEFAULT = 0.5f;
  /** Decimals for the value of score. */
  public static final int DECIMALS = 4;
  /** Number used in the keepXdecimals method. */
  public static final int TEN = 10;
  /** Comparator to sort clusters in the list based on their score. */
  private final ClusterScoreComparator scoreComparator =
    new ClusterScoreComparator();

  /**
   * Compute method.
   * @param messages Messages received
   */
  public final void compute(Vertex<LongWritable, 
      SemiClusterTreeSetWritable, DoubleWritable> vertex, 
      final Iterable<SemiClusterTreeSetWritable> messages) {
    // Set the number of iterations
    int iterations = getContext().getConfiguration().getInt(
      ITERATIONS_KEYWORD, ITERATIONS_DEFAULT);
    // Set the maximum number of clusters
    int clusterListCapacity =
      getContext().getConfiguration().getInt(
        CLUSTER_LIST_CAPACITY_KEYWORD, CLUSTER_LIST_CAPACITY_DEFAULT);
    // Set the number of iterations
    int clusterCapacity =
      getContext().getConfiguration().getInt(
        CLUSTER_CAPACITY_KEYWORD, CLUSTER_CAPACITY_DEFAULT);
    // Boundary Edge Score Factor [0,1] - user-defined
    double boundaryEdgeScoreFactor =
      getContext().getConfiguration().getFloat(
        SCORE_FACTOR_KEYWORD, SCORE_FACTOR_DEFAULT);

    // In the first superstep:
    // 1. Create an empty list of clusters
    // 2. Create an empty cluster
    // 3. Add vertex in the empty cluster
    // 4. Add cluster in the list of clusters
    // 5. Send list of clusters to neighbors
    if (getSuperstep() == 0) {
      SemiClusterTreeSetWritable clusterList =
        new SemiClusterTreeSetWritable(scoreComparator);
      SemiCluster myCluster = new SemiCluster();
      myCluster.addVertex(vertex, boundaryEdgeScoreFactor);
      clusterList.add(myCluster);
      vertex.setValue(clusterList);
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        SemiClusterTreeSetWritable message = new SemiClusterTreeSetWritable();
        message.addAll(vertex.getValue());
        sendMessage(edge.getTargetVertexId(), message);
      }
      vertex.voteToHalt();
      return;
    }

    if (getSuperstep() == iterations) {
      vertex.voteToHalt();
      return;
    }
    // In the next supersteps:
    // For each message/list, for each cluster in the list
    // if vertex is NOT part of the cluster:
    // 1. Duplicate cluster c,
    // 2. Add vertex to the duplicated cluster c' & compute score for c'
    // 3. Sort all clusters (list)
    // 4. Send the list to neighbors

    SemiClusterTreeSetWritable tempList =
      new SemiClusterTreeSetWritable(scoreComparator);
    tempList.addAll(vertex.getValue());

    for (SemiClusterTreeSetWritable  message : messages) {
      for (SemiCluster cluster: message) {
        if (!cluster.verticesList.contains(vertex.getId())
          && cluster.verticesList.size() < clusterCapacity) {
          SemiCluster newCluster = new SemiCluster(cluster);
          newCluster.addVertex(vertex, boundaryEdgeScoreFactor);
          boolean added = tempList.add(newCluster);
        }
      }
    }
    vertex.getValue().clear();
    SemiClusterTreeSetWritable value =
      new SemiClusterTreeSetWritable(scoreComparator);
    Iterator<SemiCluster> iterator = tempList.iterator();
    for (int i = 0; i < clusterListCapacity; i++) {
      if (iterator.hasNext()) {
        value.add(iterator.next());
        vertex.setValue(value);
      }
    }

    SemiClusterTreeSetWritable message = new SemiClusterTreeSetWritable();
    message.addAll(vertex.getValue());
    for (SemiCluster c: message) {
      System.out.println(c.toString());
    }
    sendMessageToAllEdges(vertex, message);
    vertex.voteToHalt();
  }

  /** 
   * Utility class for facilitating the sorting of the cluster list. 
   */
  private class ClusterScoreComparator implements Comparator<SemiCluster> {
    @Override
    /**
     * Compare two objects for order.
     * @param o1 the first object
     * @param o2 the second object
     *
     * @return -1 if score for object1 is smaller than score of object2,
     * 1 if score for object2 is smaller than score of object1
     * or 0 if both scores are the same
     */
    public int compare(final SemiCluster o1, final SemiCluster o2) {
      if (o1.score < o2.score) {
        return 1;
      } else if (o1.score > o2.score) {
        return -1;
      } else {
        if (!o1.equals(o2)) {
          return 1;
        }
      }
      return 0;
    }
  } 
  
  /**
   * Utility class for delivering the array of clusters
   * THIS vertex belongs to.
   */
  public static class SemiClusterTreeSetWritable extends TreeSet<SemiCluster>
  implements WritableComparable<SemiClusterTreeSetWritable> {
    
    private static final long serialVersionUID = 2187166892916963064L;

    /**
     * Default Constructor.
     */
    public SemiClusterTreeSetWritable() {
      super();
    }

    /**
     * Constructor.
     *
     * @param c Comparator object to sort clusterList with the score
     */
    public SemiClusterTreeSetWritable(final ClusterScoreComparator c) {
      super(c);
    }

    /** Add a semi cluster in the list of clusters.
     *
     * @param c Semi cluster to be added
     */
    final void addCluster(final SemiCluster c) {
      add(c);
    }

    /**
     * CompareTo method.
     * Two lists of semi clusters are the same when:
     * (i) they have the same number of clusters,
     * (ii) all their clusters are the same, i.e. contain the same vertices
     * For each cluster, check if it exists in the other list of semi clusters
     *
     * @param other A list with clusters to be compared with the current list
     *
     * @return return 0 if two lists are the same
     */
    public final int compareTo(final SemiClusterTreeSetWritable other) {
      if (this.size() < other.size()) {
        return -1;
      }
      if (this.size() > other.size()) {
        return 1;
      }
      Iterator<SemiCluster> iterator1 = this.iterator();
      Iterator<SemiCluster> iterator2 = other.iterator();
      while (iterator1.hasNext()) {
        if (iterator1.next().compareTo(iterator2.next()) != 0) {
          return -1;
        }
      }
      return 0;
    }

    /**
     * Read Fields.
     *
     * @param input Input to be read.
     * @throws IOException for IO.
     */
    @Override
    public final void readFields(final DataInput input) throws IOException {
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        SemiCluster c = new SemiCluster();
        c.readFields(input);
        add(c);
      }
    }

    /**
     * Write Fields.
     *
     * @param output Output to be written.
     * @throws IOException for IO.
     */
    @Override
    public final void write(final DataOutput output) throws IOException {
      output.writeInt(size());
      for (SemiCluster c : this) {
        c.write(output);
      }
    }

    /**
     * Convert object to String object.
     *
     * @return String object.
     */
    @Override
    public final String toString() {
      StringBuilder builder = new StringBuilder();
      for (SemiCluster v: this) {
        builder.append(v.toString() + " ");
      }
      builder.append("\n");
      return builder.toString();
    }
  }

  /**
   * This class represents a semi-cluster.
   */
  public static class SemiCluster
    implements Writable, Comparable<SemiCluster> {

    /** List of Vertices belonged to current semi cluster. */
    private HashSet<LongWritable> verticesList;
    /** Score of current semi cluster. */
    private double score;
    /** Inner Score. */
    private double innerScore;
    /** Boundary Score. */
    private double boundaryScore;

    /** Constructor: Create a new empty Cluster. */
    public SemiCluster() {
      verticesList = new HashSet<LongWritable>();
      score = 1d;
      innerScore = 0d;
      boundaryScore = 0d;
    }

    /**
     * Constructor: Initialize a new Cluster.
     *
     * @param cluster cluster object to initialize the new object
     */
    public SemiCluster(final SemiCluster cluster) {
      verticesList = new HashSet<LongWritable>();
      verticesList.addAll(cluster.verticesList);
      score = cluster.score;
      innerScore = cluster.innerScore;
      boundaryScore = cluster.boundaryScore;
    }

    /**
     * Add a vertex to the cluster and recalculate the score.
     *
     * @param vertex The new vertex to be added into the cluster
     * @param boundaryEdgeScoreFactor Boundary Edge Score Factor
     */
    public final void addVertex(
        final Vertex<LongWritable, ? , DoubleWritable> vertex, 
        final double boundaryEdgeScoreFactor) {
      long vertexId = vertex.getId().get();
      if (verticesList.add(new LongWritable(vertexId))) {
          this.computeScore(vertex, boundaryEdgeScoreFactor);
          score = keepXdecimals(score, DECIMALS);
          innerScore = keepXdecimals(innerScore, DECIMALS);
          boundaryScore = keepXdecimals(boundaryScore, DECIMALS);
      }
    }
    /**
     * Get size of semi cluster list.
     *
     * @return Number of semi clusters in the list
     */
    public final int getSize() {
      return verticesList.size();
    }

    /**
     * Compute the score of the semi-cluster.
     *
     * @param vertex The recently added vertex
     * @param boundaryEdgeScoreFactor Boundary Edge Score Factor
     *
     */
    private void computeScore(
        final Vertex<LongWritable, ?, DoubleWritable> vertex, 
        final double boundaryEdgeScoreFactor) {
      if (getSize() == 1) {
        score = 1d;
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
          boundaryScore += edge.getValue().get();
        }
      } else {
        for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
          if (verticesList.contains(edge.getTargetVertexId())) {
            innerScore += edge.getValue().get();
            boundaryScore -= edge.getValue().get();
          } else {
            boundaryScore += edge.getValue().get();
          }
        }
      }
    }

    /**
     * Return the list of vertices belonging to current semi cluster.
     *
     * @return List of vertices belonging to current semi cluster
     */
    public final HashSet<LongWritable> getVerticesList() {
      return verticesList;
    }

    /** CompareTo method.
     * Two semi clusters are the same when:
     * (i) they have the same number of vertices,
     * (ii) all their vertices are the same
     * For each vertex, check if it exists in the other cluster
     *
     * @param other Cluster to be compared with current cluster
     *
     * @return 0 if two clusters are the same
     */
    @Override
    public final int compareTo(final SemiCluster other) {
      if (other == null) {
        return 1;
      }
      if (this.getSize() < other.getSize()) {
        return -1;
      }
      if (this.getSize() > other.getSize()) {
        return 1;
      }
      if (other.verticesList.containsAll(verticesList)) {
        return 0;
      }
      return -1;
    }

    /**
     * hashCode method.
     *
     * @return result HashCode result
     */
    @Override
    public final int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result;
      if (verticesList != null) {
        result += verticesList.hashCode();
      }
      return result;
    }

    /**
     * Equals method.
     *
     * @param obj Object to compare if it is equal with.
     * @return boolean result.
     */
    @Override
    public final boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      SemiCluster other = (SemiCluster) obj;
      if (verticesList == null) {
        if (other.verticesList != null) {
          return false;
        }
      } else if (!verticesList.equals(other.verticesList)) {
        return false;
      }
      return true;
    }

    /**
     * Convert object to string object.
     *
     * @return string object
     */
    @Override
    public final String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("[ ");
      for (LongWritable v: this.verticesList) {
        builder.append(v.toString());
        builder.append(" ");
      }
      builder.append(" | " + score + ", " + innerScore + ", "
      + boundaryScore + " ]");
      return builder.toString();
    }

    /**
     * Decimal Precision of latent vector values.
     *
     * @param value Value to be truncated
     * @param x Number of decimals to keep
     *
     * @return value with X decimals
     */
    public final double keepXdecimals(final Double value, final int x) {
      return Math.round(value * Math.pow(TEN, x - 1)) / Math.pow(TEN, x - 1);
    }

    /**
     * Read fields.
     *
     * @param input Input to be read.
     * @throws IOException for IO.
     */
    @Override
    public final void readFields(final DataInput input) throws IOException {
      verticesList = new HashSet<LongWritable>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        LongWritable e = new LongWritable();
        e.readFields(input);
        verticesList.add(e);
      }
      score = input.readDouble();
      innerScore = input.readDouble();
      boundaryScore = input.readDouble();
    }

    /**
     * Write fields.
     *
     * @param output Output to be written.
     * @throws IOException for IO.
     */
    @Override
    public final void write(final DataOutput output) throws IOException {
      output.writeInt(getSize());
      for (LongWritable vertex: verticesList) {
        vertex.write(output);
      }
      output.writeDouble(score);
      output.writeDouble(innerScore);
      output.writeDouble(boundaryScore);
    }
  }
}