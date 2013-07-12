package es.tid.graphlib.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.clustering.SemiClustering.SemiClusterTreeSetWritable;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Stochastic Gradient Descent (SGD)",
  description = "Minimizes the error in predictions of ratings")

public class SemiClustering extends Vertex<LongWritable,
SemiClusterTreeSetWritable, DoubleWritable, SemiClusterTreeSetWritable> {

  /** Keyword for parameter setting the number of iterations */
  public static final String ITERATIONS_KEYWORD = "semi.iterations";
  /** Default value for ITERATIONS */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Maximum number of Semi-Clusters */
  public static final String CLUSTER_LIST_CAPACITY_KEYWORD =
    "semi.clusters.max";
  /** Default value for Maximum Number of Semi-Clusters */
  public static final int CLUSTER_LIST_CAPACITY_DEFAULT = 2;
  /** Keyword for parameter setting the Maximum No of vertices in a cluster */
  public static final String CLUSTER_CAPACITY_KEYWORD =
    "semi.cluster.capacity";
  /** Default value for CAPACITY */
  public static final int CLUSTER_CAPACITY_DEFAULT = 4;
  /** Keyword for parameter setting the Boundary Edge Score Factor */
  public static final String SCORE_FACTOR_KEYWORD = "semi.score.factor";
  /** Default value for Boundary Edge Score Factor */
  public static final float SCORE_FACTOR_DEFAULT = 0.5f;
  /** Comparator to sort clusters in the list based on their score */
  private final ClusterScoreComparator scoreComparator =
    new ClusterScoreComparator();
  /** Boundary Edge Score Factor [0,1] - user-defined */
  private double boundaryEdgeScoreFactor;

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<SemiClusterTreeSetWritable> messages) {
    // Set the number of iterations
    int iterations = getContext().getConfiguration().getInt(ITERATIONS_KEYWORD,
      ITERATIONS_DEFAULT);
    // Set the maximum number of clusters
    int clusterListCapacity =
      getContext().getConfiguration().getInt(CLUSTER_LIST_CAPACITY_KEYWORD,
        CLUSTER_LIST_CAPACITY_DEFAULT);
    // Set the number of iterations
    int clusterCapacity =
      getContext().getConfiguration().getInt(CLUSTER_CAPACITY_KEYWORD,
        CLUSTER_CAPACITY_DEFAULT);
    // Set the boundary edge score factor
    boundaryEdgeScoreFactor =
      getContext().getConfiguration().getFloat(SCORE_FACTOR_KEYWORD,
        SCORE_FACTOR_DEFAULT);

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
      myCluster.addVertex(this, boundaryEdgeScoreFactor);
      clusterList.add(myCluster);
      setValue(clusterList);
      System.out.println("---S: " + getSuperstep() + ", id: " + getId() +
        ", clusterList: " + getValue() + ", clusterList size: " + getValue().size());
      for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
        sendMessage(edge.getTargetVertexId(), getValue());
      }
      voteToHalt();
      return;
    }

    if (getSuperstep() == iterations) {
      voteToHalt();
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
    tempList.addAll(getValue());
    // FOR LOOP - for each message/list
    for (SemiClusterTreeSetWritable  message : messages) {
      System.out.println("S:" + getSuperstep() + ", id:" + getId() +
        ", message received:" + message.toString());
      for (SemiCluster cluster: message) {
      //Iterator<SemiCluster> clusterIter = message.iterator();
      // WHILE LOOP - for each cluster in the message/list
      //while (clusterIter.hasNext()) {
        //SemiCluster cluster = new SemiCluster(clusterIter.next());
        if (!cluster.verticesList.contains(getId()) &&
          cluster.verticesList.size() < clusterCapacity) {
          SemiCluster newCluster = new SemiCluster(cluster);
          newCluster.addVertex(this, boundaryEdgeScoreFactor);
          tempList.add(newCluster);
        }
      } // END OF WHILE LOOP - (for each cluster)
    } // END OF FOR LOOP - (for each message/list)
    getValue().clear();
    SemiClusterTreeSetWritable value =
      new SemiClusterTreeSetWritable(scoreComparator);
    Iterator<SemiCluster> iterator = tempList.iterator();
    for (int i = 0; i < clusterListCapacity; i++) {
      if (iterator.hasNext()) {
        value.add(iterator.next());
        setValue(value);
      }
    }
    System.out.println("---S: " + getSuperstep() + ", id: " + getId() +
      ", clusterList: " + getValue() + ", clusterList size: " + getValue().size());

    for (Edge<LongWritable, DoubleWritable> edge : getEdges()) {
      sendMessage(edge.getTargetVertexId(), getValue());
    }
    voteToHalt();
  } // END OF Compute()

  /** Utility class for facilitating the  sorting of the cluster list */
  private class ClusterScoreComparator implements Comparator<SemiCluster> {
    @Override
    public int compare(SemiCluster o1, SemiCluster o2) {
      if (o1.score < o2.score) {
        return +1;
      } else if (o1.score > o2.score) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Utility class for delivering the array of clusters THIS vertex belongs to
   */
  public static class SemiClusterTreeSetWritable extends TreeSet<SemiCluster>
  implements WritableComparable<SemiClusterTreeSetWritable> {
    /**
     * Default Constructor
     */
    public SemiClusterTreeSetWritable() {
      super();
      //clusterList = new TreeSet<SemiCluster>();
    }

    /**
     * Constructor
     *
     * @param c Comparator object to sort clusterList with the score
     */
    public SemiClusterTreeSetWritable(ClusterScoreComparator c) {
      super(c);
      //clusterList = new TreeSet<SemiCluster>();
    }

    /** Add a semi cluster in the list of clusters
     *
     * @param c Semi cluster to be added
     */
    void addCluster(SemiCluster c) {
      add(c);
    }

    /** CompareTo
     * Two lists of semi clusters are the same when:
     * --> they have the same number of clusters
     * --> all their clusters are the same, i.e. contain the same vertices
     * For each cluster, check if it exists in the other list of semi clusters
     *
     * @param other A list with clusters to be compared with the current list
     *
     * @return return 0 if two lists are the same
     */
    public int compareTo(SemiClusterTreeSetWritable other) {
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

    @Override
    public void readFields(DataInput input) throws IOException {
      //clusterList = new TreeSet<SemiCluster>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        SemiCluster c = new SemiCluster();
        c.readFields(input);
        add(c);
      }
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeInt(size());
      for (SemiCluster c : this) {
        c.write(output);
      }
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (SemiCluster c: this) {
        builder.append("[ ");
        for (LongWritable v: c.verticesList) {
          builder.append(v.toString());
          builder.append(' ');
        }
        builder.append(" ] ");
      }
      return builder.toString();
    }
  } // END OF class SemiClusterTreeSetWritable

  /**
   * Class SemiCluster
   */
  public static class SemiCluster implements Writable, Comparable<SemiCluster> {
    /** List of Vertices belonged to current semi cluster */
    private HashSet<LongWritable> verticesList;
    /** Score of current semi cluster */
    private double score;


    /** Constructor: Create a new empty Cluster */
    public SemiCluster() {
      verticesList = new HashSet<LongWritable>();
      score = 1d;
    }

    /**
     * Constructor: Initialize a new Cluster
     *
     * @param cluster cluster object to initialize the new object
     */
    public SemiCluster(SemiCluster cluster) {
      verticesList = new HashSet<LongWritable>();
      verticesList.addAll(cluster.verticesList);
      score = cluster.score;
    }

    /**
     * Add a vertex to the cluster and recalculate the score
     *
     * @param vertex The new vertex to be added into the cluster
     */
    public void addVertex(Vertex<LongWritable, ?, DoubleWritable, ?>
    vertex, double boundaryEdgeScoreFactor) {
      long vertexId = vertex.getId().get();
      if (verticesList.add(new LongWritable(vertexId))) {
        score = computeScore(vertex, boundaryEdgeScoreFactor);
      }
    }

    /**
     * Get size of semi cluster list.
     *
     * @return Number of semi clusters in the list
     */
    public int getSize() {
      return verticesList.size();
    }

    /**
     * Compute the score of the semi-cluster
     *
     * @param vertex The recently added vertex
     *
     * @return the new score of the cluster
     */
    private double computeScore(Vertex<LongWritable, ?, DoubleWritable, ?>
    vertex, double boundaryEdgeScoreFactor) {
      if (getSize() == 1) {
        return 1d;
      }
      double innerScore = 0d;
      double boundaryScore = 0d;
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        double targetVertexId = edge.getTargetVertexId().get();
        if (verticesList.contains(targetVertexId)) {
          innerScore += edge.getValue().get();
        } else {
          boundaryScore += edge.getValue().get();
        }
      }
      return (innerScore - (boundaryEdgeScoreFactor * boundaryScore)) /
        (getSize() * (getSize() - 1) / 2);
    } // End of computeScore()

    /**
     * Return the list of vertices belonging to current semi cluster
     * @return List of vertices belonging to current semi cluster
     */
    public HashSet<LongWritable> getVerticesList() {
      return verticesList;
    }

    /** CompareTo
     * Two semi clusters are the same when:
     * --> they have the same number of vertices
     * --> all their vertices are the same
     * For each vertex, check if it exists in the other cluster
     *
     * @param cluster Cluster to be compared with current cluster
     *
     * @return 0 if two clusters are the same
     */
    @Override
    public int compareTo(SemiCluster other) {
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

    @Override
    public void readFields(DataInput input) throws IOException {
      verticesList = new HashSet<LongWritable>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        LongWritable e = new LongWritable();
        e.readFields(input);
        verticesList.add(e);
      }
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeInt(getSize());
      for (LongWritable vertex: verticesList) {
        vertex.write(output);
      }
    }
  } // End of SemiCluster Child Class
} // End of SemiClustering Parent Class