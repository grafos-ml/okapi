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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.clustering.SemiClustering.SemiClusterTreeSetWritable;

/**
 * Demonstrates the Pregel Stochastic Gradient Descent (SGD) implementation.
 */
@Algorithm(
  name = "Stochastic Gradient Descent (SGD)",
  description = "Minimizes the error in predictions of ratings")

public class SemiClustering extends Vertex<IntWritable,
SemiClusterTreeSetWritable , DoubleWritable, SemiClusterTreeSetWritable> {

  /** Keyword for parameter setting the number of iterations */
  public static final String ITERATIONS_KEYWORD = "semi.iterations";
  /** Default value for ITERATIONS */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the Maximum number of Semi-Clusters */
  public static final String CLUSTER_LIST_CAPACITY_KEYWORD = "semi.clusters.max";
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

  private final ClusterScoreComparator scoreComparator = 
    new ClusterScoreComparator();
  
  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<SemiClusterTreeSetWritable > messages) {
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
    // In the first superstep:
    // 1. Create an empty list of clusters
    // 2. Create an empty cluster
    // 3. Add vertex in the empty cluster
    // 4. Add cluster in the list of clusters
    // 5. Send list of clusters to neighbors
    if (getSuperstep() == 0) {
      SemiClusterTreeSetWritable  clusterList =
        new SemiClusterTreeSetWritable(scoreComparator);
      SemiCluster myCluster = new SemiCluster();
      myCluster.addVertex(this);
      clusterList.add(myCluster);
      setValue(clusterList);
      for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
        sendMessage(edge.getTargetVertexId(), getValue());
      }
      voteToHalt();
    }

    // In the next supersteps:
    // For each message/list, for each cluster in the list
    // if vertex is NOT part of the cluster:
    // 1. Duplicate cluster c, 
    // 2. Add vertex to the duplicated cluster c' & compute score for c'
    // 3. Sort all clusters (list)
    // 4. Send the list to neighbors
    if (getSuperstep() < iterations) {
      SemiClusterTreeSetWritable tempList =
        new SemiClusterTreeSetWritable(new ClusterScoreComparator());
      tempList.addAll(getValue());
      // FOR LOOP - for each message
      for (SemiClusterTreeSetWritable  message : messages) {
        Iterator<SemiCluster> clusterIter = message.clusterList.iterator();
        while (clusterIter.hasNext()) {
          SemiCluster cluster = clusterIter.next();
          if (!cluster.verticesList.contains(getId()) &&
            cluster.verticesList.size() < clusterCapacity) {
            SemiCluster newCluster = new SemiCluster(cluster);
            newCluster.addVertex(this);
            tempList.add(newCluster);
          }
        }
      } // END OF FOR LOOP - (for each message)
      getValue().clear();
      Iterator<SemiCluster> iterator = tempList.iterator();
      for (int i = 0; i < clusterListCapacity; i++) {
        if (iterator.hasNext()) {
          getValue().add(iterator.next());
        }
      }
      for (Edge<IntWritable, DoubleWritable> edge : getEdges()) {
        sendMessage(edge.getTargetVertexId(), getValue());
      }
      voteToHalt();
    }
  } // END OF Compute()

  private class ClusterScoreComparator implements Comparator<SemiCluster> {
    @Override
    public int compare(SemiCluster o1, SemiCluster o2) {
      if (o1.score < o2.score) {
        return -1;
      } else if (o1.score > o2.score) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Utility class for delivering the array of clusters THIS vertex belongs to
   */
  public class SemiClusterTreeSetWritable extends TreeSet<SemiCluster> 
  implements WritableComparable<SemiClusterTreeSetWritable>{
    /** Set with clusters*/
    TreeSet<SemiCluster> clusterList;

    /** Constructor */
    public SemiClusterTreeSetWritable (Comparator c) {
      super(c);
    }

    /** Add a semi cluster in the list of clusters
     * 
     * @param c Semi cluster to be added
     */
    void addCluster(SemiCluster c){
      clusterList.add(c);
    }

    /** CompareTo
     * Two lists of semi clusters are the same when:
     * --> they have the same number of clusters
     * --> all their clusters are the same, i.e. contain the same vertices
     * For each cluster, check if it exists in the other list of semi clusters
     */
    public int compareTo(SemiClusterTreeSetWritable other) {
      if (this.size() < other.size()) {
        return -1;
      }
      if (this.size() > other.size()) {
        return 1;
      }
      boolean exists = false;
      Iterator<SemiCluster> iterator1 = clusterList.iterator();
      while (iterator1.hasNext()) {
        Iterator<SemiCluster> iterator2 = other.clusterList.iterator();
        while (iterator2.hasNext()) {
          if (iterator1.next().compareTo(iterator2.next()) == 0) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          return -1;
        } else {
          exists = false;
        }
      }
      return 0;
    }

    /**
     * Get size of semi cluster list.
     *
     * @return Number of semi clusters in the list
     */
    public int getSize() {
      return clusterList.size();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      clusterList = new TreeSet<SemiCluster>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        SemiCluster c = new SemiCluster();
        c.readFields(input);
        clusterList.add(c);
      }
      
    }

    @Override
    public void write(DataOutput output) throws IOException {
      Iterator<SemiCluster> cluster = clusterList.iterator();
      while (cluster.hasNext()) {
        SemiCluster c = cluster.next();
        output.writeInt(c.getSize());
        c.write(output);
      }
    }
  }
  /**
   * Class SemiCluster
   */
  public class SemiCluster implements Writable, Comparable<SemiCluster> {
    /** List of Vertices belonged to current semi cluster */
    private HashSet<IntWritable> verticesList;
    /** Score of current semi cluster */
    private double score;
    /** Boundary Edge Score Factor [0,1] - user-defined */
    private double boundaryEdgeScoreFactor;

    /** Constructor: Create a new empty Cluster */
    public SemiCluster() {
      verticesList = new HashSet<IntWritable>();
      score = 1d;
      boundaryEdgeScoreFactor = 0d;
    }

    /**
     * Constructor: Initialize a new Cluster
     *
     * @param vertices List of vertices belonging to semi cluster
     * @param score Score of semi cluster
     * @param size Size of semi cluster
     * @param boundaryEdgeScoreFactor Boundary edge score factor: user-defined
     */
    public SemiCluster(SemiCluster cluster) {
      verticesList.addAll(cluster.verticesList);
      this.score = cluster.score;
      this.boundaryEdgeScoreFactor = cluster.boundaryEdgeScoreFactor;
    }

    /**
     * Add a vertex to the cluster and recalculate the score
     *
     * @param vertex The new vertex to be added into the cluster
     */
    public void addVertex(Vertex<IntWritable, ?, DoubleWritable, ?>
    vertex) {
      int vertexId = vertex.getId().get();
      if (verticesList.add(new IntWritable(vertexId))) {
        score = computeScore(vertex);
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
    private double computeScore(Vertex<IntWritable, ?, DoubleWritable, ?>
    vertex) {
      // Set the boundary edge score factor
      boundaryEdgeScoreFactor =
        getContext().getConfiguration().getFloat(SCORE_FACTOR_KEYWORD,
          SCORE_FACTOR_DEFAULT);

      if (getSize() == 1) {
        return 1d;
      }
      double innerScore = 0d;
      double boundaryScore = 0d;
      for (Edge<IntWritable, DoubleWritable> edge : vertex.getEdges()) {
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
    public HashSet<IntWritable> getVerticesList(){
      return verticesList;
    }

    /** CompareTo
    * Two semi clusters are the same when:
    * --> they have the same number of vertices
    * --> all their vertices are the same
    * For each vertex, check if it exists in the other cluster
    */
    @Override
    public int compareTo(SemiCluster cluster) {
      if (cluster == null) {
        return 1;
      }
      if (this.getSize() < cluster.getSize()) {
        return -1;
      }
      if (this.getSize() > cluster.getSize()) {
        return 1;
      }
      Iterator<IntWritable> iterator1 = verticesList.iterator();
      boolean exists = false;
      while (iterator1.hasNext()) {
        Iterator<IntWritable> iterator2 = cluster.verticesList.iterator();
        while (iterator2.hasNext()) {
          if (iterator1.next().get() == iterator2.next().get()) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          return -1;
        } else {
          exists = false;
        }
      }
      return 0;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      verticesList = new HashSet<IntWritable>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        IntWritable e = new IntWritable();
        e.readFields(input);
        verticesList.add(e);
      }
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeInt(getSize());
      Iterator<IntWritable> iterator = verticesList.iterator();
      while (iterator.hasNext()) {
        iterator.next().write(output);
      }
    }
  } // End of SemiCluster Child Class
} // End of SemiClustering Parent Class