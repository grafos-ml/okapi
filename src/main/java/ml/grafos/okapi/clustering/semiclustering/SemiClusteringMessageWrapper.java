package ml.grafos.okapi.clustering.semiclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import ml.grafos.okapi.clustering.semiclustering.SemiClusteringMessageWrapper.SemiClusterTreeSetWritable;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Demonstrates the Semi Clustering implementation.
 */
@Algorithm(
  name = "Semi Clustering",
  description = "It puts vertices into clusters")

public class SemiClusteringMessageWrapper extends BasicComputation<LongWritable,
SemiClusterTreeSetWritable, DoubleWritable, MessageWrapper> {

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

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(
      Vertex<LongWritable, SemiClusterTreeSetWritable, DoubleWritable> vertex,
      Iterable<MessageWrapper> messages) {
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
    // Boundary Edge Score Factor [0,1] - user-defined
    double boundaryEdgeScoreFactor =
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
      myCluster.addVertex(vertex, boundaryEdgeScoreFactor);
      clusterList.add(myCluster);
      vertex.setValue(clusterList);
      System.out.println("---S: " + getSuperstep() + ", id: " + vertex.getId() +
        ", clusterList: " + vertex.getValue());
      //sendMessageToAllEdges(getValue());
      for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        MessageWrapper message = new MessageWrapper();
        message.setSourceId(vertex.getId());
        message.setMessage(vertex.getValue());

        sendMessage(edge.getTargetVertexId(), message);
        //System.out.println("send " + message.getMessage().toString() +
        //" to " + edge.getTargetVertexId());
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

    // FOR LOOP - for each message/list
    for (MessageWrapper  message : messages) {
      System.out.println("S:" + getSuperstep() + ", id:" + vertex.getId() +
        ", message received " + message.getMessage().toString());
      for (SemiCluster cluster: message.getMessage()) {
      //Iterator<SemiCluster> clusterIter = message.iterator();
      // WHILE LOOP - for each cluster in the message/list
      //while (clusterIter.hasNext()) {
        //SemiCluster cluster = new SemiCluster(clusterIter.next());
        if (!cluster.verticesList.contains(vertex.getId()) &&
          cluster.verticesList.size() < clusterCapacity) {
          SemiCluster newCluster = new SemiCluster(cluster);
          newCluster.addVertex(vertex, boundaryEdgeScoreFactor);
          boolean added = tempList.add(newCluster);
          //tempList.addCluster(newCluster);
          System.out.println("Cluster: " + cluster.verticesList.toString() +
            ", newCluster: " + newCluster.toString() + "added: " + added +
            ", tempList: " + tempList.toString());
        }
      } // END OF WHILE LOOP - (for each cluster)
    } // END OF FOR LOOP - (for each message/list)
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
    System.out.println("---S: " + getSuperstep() + ", id: " + vertex.getId() +
      ", clusterList: " + vertex.getValue());

    MessageWrapper message = new MessageWrapper();
    message.setSourceId(vertex.getId());
    message.setMessage(vertex.getValue());
    for (SemiCluster c: message.getMessage()) {
      System.out.println(c.toString());
    }
    sendMessageToAllEdges(vertex, message);
    vertex.voteToHalt();
  } // END OF Compute()

  /***************************************************************************
   ***************************************************************************
   ***************************************************************************
   */
  /** Utility class for facilitating the  sorting of the cluster list */
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
    public int compare(SemiCluster o1, SemiCluster o2) {
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
  } // End of class ClusterScoreComparator
  /***************************************************************************
   ***************************************************************************
   ***************************************************************************
   */
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

    /** Equals
     * Two lists of semi clusters are equal when:
     * --> they have the same number of clusters
     * --> all their clusters are the same, i.e. contain the same vertices
     * For each cluster, check if it exists in the other list of semi clusters
     *
     * @param other A list with clusters to be compared if it is equal
     * with the current list
     *
     * @return return true if two lists are equal, otherwise false
     */
/*
    public boolean equals2(SemiClusterTreeSetWritable other) {
      if (this.size() < other.size() || this.size() > other.size()) {
        return false;
      }
      Iterator<SemiCluster> iterator1 = this.iterator();
      Iterator<SemiCluster> iterator2 = other.iterator();
      while (iterator1.hasNext()) {
        if (!iterator1.next().equals(iterator2.next())) {
          return false;
        }
      }
      return true;
    }
*/

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
/*
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
    }*/
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (SemiCluster v: this) {
        builder.append(v.toString() + " ");
      }
      builder.append("\n");
      return builder.toString();
    }
  } // END OF class SemiClusterTreeSetWritable

 /****************************************************************************
  ****************************************************************************
  ****************************************************************************
  */
  /**
   * Class SemiCluster
   */
  public static class SemiCluster
    implements Writable, Comparable<SemiCluster> {

    /** List of Vertices belonged to current semi cluster */
    private HashSet<LongWritable> verticesList;
    /** Score of current semi cluster */
    private double score;
    /** Inner Score */
    private double innerScore;
    /** Boundary Score */
    private double boundaryScore;

    /** Constructor: Create a new empty Cluster */
    public SemiCluster() {
      verticesList = new HashSet<LongWritable>();
      score = 1d;
      innerScore = 0d;
      boundaryScore = 0d;
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
      innerScore = cluster.innerScore;
      boundaryScore = cluster.boundaryScore;
    }

    /**
     * Add a vertex to the cluster and recalculate the score
     *
     * @param vertex The new vertex to be added into the cluster
     */
    public void addVertex(Vertex<LongWritable, ?, DoubleWritable> vertex, 
        double boundaryEdgeScoreFactor) {
      long vertexId = vertex.getId().get();
      if (verticesList.add(new LongWritable(vertexId))) {
          this.computeScore(vertex, boundaryEdgeScoreFactor);
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
    private void computeScore(Vertex<LongWritable, ?, DoubleWritable> vertex, 
        double boundaryEdgeScoreFactor) {
      /*for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
        System.out.println("v:" + vertex.getId() + ", target:" +
          edge.getTargetVertexId() + ", edgeValue:" + edge.getValue());
      }*/
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
        System.out.println("inner: " + innerScore + " - (boundary: " +
          boundaryScore + " * " + boundaryEdgeScoreFactor + ") / size:" + getSize());
        score = (innerScore - (boundaryEdgeScoreFactor * boundaryScore)) /
          (getSize() * (getSize() - 1) / 2);
      }
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
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
        + ((verticesList == null) ? 0 : verticesList.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
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

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("[ ");
      for (LongWritable v: this.verticesList) {
        builder.append(v.toString());
        builder.append(" ");
      }
      builder.append(" | " + score + ", " + innerScore + ", " + boundaryScore + " ]");
      return builder.toString();
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
      score = input.readDouble();
      innerScore = input.readDouble();
      boundaryScore = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeInt(getSize());
      for (LongWritable vertex: verticesList) {
        vertex.write(output);
      }
      output.writeDouble(score);
      output.writeDouble(innerScore);
      output.writeDouble(boundaryScore);
    }
  } // End of SemiCluster Child Class
} // End of SemiClustering Parent Class