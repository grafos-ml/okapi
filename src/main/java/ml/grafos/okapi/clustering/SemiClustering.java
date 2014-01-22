package ml.grafos.okapi.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import ml.grafos.okapi.clustering.SemiClustering.SemiClusterTreeSetWritable;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;


/**
 * Implements the Semi-Clustering algorithm as presented in the Pregel paper
 * from SIGMOD'10.
 * 
 * The input to the algorithm is an undirected weighted graph and the output 
 * is a set of clusters with each vertex potentially belonging to multiple
 * clusters.
 * 
 * A semi-cluster is assigned a score S=(I-f*B)/(V(V-1)/2), where I is the sum
 * of weights of all internal edges, B is the sum of weights of all boundary
 * edges, V is the number of vertices in the semi-cluster, f is a user-specified
 * boundary edge score factor with a value between 0 and 1. 
 * 
 * Each vertex maintains a list containing a maximum number of  semi-clusters, 
 * sorted by score. The lists gets greedily updated in an iterative manner.
 * 
 * The algorithm finishes when the semi-cluster lists don't change or after a
 * maximum number of iterations.
 * 
 */
@Algorithm(
    name = "Semi Clustering",
    description = "Semi-cluster implementation")

public class SemiClustering extends BasicComputation<LongWritable,
SemiClusterTreeSetWritable, DoubleWritable, SemiClusterTreeSetWritable> {

  /** Maximum number of iterations. */
  public static final String ITERATIONS = "iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Maximum number of semi-clusters. */
  public static final String MAX_CLUSTERS = "max.clusters";
  /** Default value for maximum number of semi-clusters. */
  public static final int MAX_CLUSTERS_DEFAULT = 2;
  /** Maximum number of vertices in a semi-cluster. */
  public static final String CLUSTER_CAPACITY = "cluster.capacity";
  /** Default value for cluster capacity. */
  public static final int CLUSTER_CAPACITY_DEFAULT = 4;
  /** Boundary edge score factor. */
  public static final String SCORE_FACTOR = "score.factor";
  /** Default value for Boundary Edge Score Factor. */
  public static final float SCORE_FACTOR_DEFAULT = 0.5f;
  /** Comparator to sort clusters in the list based on their score. */
  private static final ClusterScoreComparator scoreComparator =
      new ClusterScoreComparator();

  /**
   * Compute method.
   * @param messages Messages received
   */
  public final void compute(Vertex<LongWritable, 
      SemiClusterTreeSetWritable, DoubleWritable> vertex, 
      final Iterable<SemiClusterTreeSetWritable> messages) {

    int iterations = getContext().getConfiguration().getInt(
        ITERATIONS, ITERATIONS_DEFAULT);
    int maxClusters = getContext().getConfiguration().getInt(
        MAX_CLUSTERS, MAX_CLUSTERS_DEFAULT);
    int clusterCapacity = getContext().getConfiguration().getInt(
        CLUSTER_CAPACITY, CLUSTER_CAPACITY_DEFAULT);
    double scoreFactor = getContext().getConfiguration().getFloat(
        SCORE_FACTOR, SCORE_FACTOR_DEFAULT);

    // If this is the first superstep, initialize cluster list with a single
    // cluster that contains only the current vertex, and send it to all 
    // neighbors.
    if (getSuperstep() == 0) {
      SemiCluster myCluster = new SemiCluster();
      myCluster.addVertex(vertex, scoreFactor);

      SemiClusterTreeSetWritable clusterList = new SemiClusterTreeSetWritable();
      clusterList.add(myCluster);

      vertex.setValue(clusterList);
      sendMessageToAllEdges(vertex, clusterList);
      vertex.voteToHalt();
      return;
    }

    if (getSuperstep() == iterations) {
      vertex.voteToHalt();
      return;
    }

    // For every cluster list received from neighbors and for every cluster in 
    // the list, add current vertex if it doesn't already exist in the cluster
    // and the cluster is not full.
    // 
    // Sort clusters received and newly formed clusters and send the top to all
    // neighbors
    // 
    // Furthermore, update this vertex's list with received and newly formed
    // clusters the contain this vertex, sort them, and keep the top ones.
    
    SemiClusterTreeSetWritable unionedClusterSet = 
        new SemiClusterTreeSetWritable();
    vertex.getValue().clear();
    
    for (SemiClusterTreeSetWritable clusterSet : messages) {
      
      unionedClusterSet.addAll(clusterSet);
      
      for (SemiCluster cluster: clusterSet) {
        boolean contains = cluster.vertices.contains(vertex.getId());
        if (!contains && cluster.vertices.size() < clusterCapacity) {
          SemiCluster newCluster = new SemiCluster(cluster);
          newCluster.addVertex(vertex, scoreFactor);
          unionedClusterSet.add(newCluster);
          vertex.getValue().add(newCluster);
        } else if (contains) {
          vertex.getValue().add(cluster);
        }
      }
    }

    // If we have more than a maximum number of clusters, then we remove the 
    // ones with the lowest score.
    Iterator<SemiCluster> iterator = unionedClusterSet.iterator();
    while (unionedClusterSet.size()>maxClusters) {
      iterator.next();
      iterator.remove();
    }

    iterator = vertex.getValue().iterator();
    while(vertex.getValue().size()>maxClusters) {
      iterator.next();
      iterator.remove();
    }

    sendMessageToAllEdges(vertex, unionedClusterSet);
    vertex.voteToHalt();
  }

  /** 
   * Comparator that sorts semi-clusters according to their score.
   */
  private static class ClusterScoreComparator 
  implements Comparator<SemiCluster> {
    /**
     * Compare two semi-clusters for order.
     * @param o1 the first object
     * @param o2 the second object
     *
     * @return -1 if score for object1 is smaller than score of object2,
     * 1 if score for object2 is smaller than score of object1
     * or 0 if both scores are the same
     */
    @Override
    public int compare(final SemiCluster o1, final SemiCluster o2) {
      if (o1.score < o2.score) {
        return -1;
      } else if (o1.score > o2.score) {
        return 1;
      } else {
        // We add this for consistency with the equals() method.
        if (!o1.equals(o2)) {
          return 1;
        }
      }
      return 0;
    }
  } 

  /**
   * A set of semi-clusters that is writable. We use a TreeSet since we want
   * the ability to sort semi-clusters.
   */
  public static class SemiClusterTreeSetWritable extends TreeSet<SemiCluster>
  implements WritableComparable<SemiClusterTreeSetWritable> {

    /**
     * Default Constructor. We ensure that this list always sorts semi-clusters
     * according to the comparator.
     */
    public SemiClusterTreeSetWritable() {
      super(scoreComparator);
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
     * Implements the readFields method of the Writable interface.
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
     * Implements the write method of the Writable interface.
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
     * Returns a string representation of the list.
     *
     * @return String object.
     */
    @Override
    public final String toString() {
      StringBuilder builder = new StringBuilder();
      for (SemiCluster v: this) {
        builder.append(v.toString() + " ");
      }
      return builder.toString();
    }
  }

  /**
   * This class represents a semi-cluster.
   */
  private static class SemiCluster implements WritableComparable<SemiCluster> {

    /** List of vertices  */
    private HashSet<LongWritable> vertices;
    /** Score of current semi cluster. */
    private double score;
    /** Inner Score. */
    private double innerScore;
    /** Boundary Score. */
    private double boundaryScore;

    /** Constructor: Create a new empty Cluster. */
    public SemiCluster() {
      vertices = new HashSet<LongWritable>();
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
      vertices = new HashSet<LongWritable>();
      vertices.addAll(cluster.vertices);
      score = cluster.score;
      innerScore = cluster.innerScore;
      boundaryScore = cluster.boundaryScore;
    }

    /**
     * Adds a vertex to the cluster.
     * 
     * Every time a vertex is added we also update the inner and boundary score.
     * Because vertices are only added to a semi-cluster, we can save the inner
     * and boundary scores and update them incrementally.
     * 
     * Otherwise, in order to re-compute it from scratch we would need every
     * vertex to send a friends-of-friends list, which is very expensive.
     * 
     * @param vertex The new vertex to be added into the cluster
     * @param scoreFactor Boundary Edge Score Factor
     */
    public final void addVertex(
        final Vertex<LongWritable, ? , DoubleWritable> vertex, 
        final double scoreFactor) {
      long vertexId = vertex.getId().get();

      if (vertices.add(new LongWritable(vertexId))) {
        if (size() == 1) {
          for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            boundaryScore += edge.getValue().get();
          }
          score = 0.0;
        } else {
          for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
            if (vertices.contains(edge.getTargetVertexId())) {
              innerScore += edge.getValue().get();
              boundaryScore -= edge.getValue().get();
            } else {
              boundaryScore += edge.getValue().get();
            }
          }
          score =  (innerScore-scoreFactor*boundaryScore)/(size()*(size()-1)/2);
        }
      }
    }

    /**
     * Returns size of semi cluster list.
     *
     * @return Number of semi clusters in the list
     */
    public final int size() {
      return vertices.size();
    }

    /** 
     * Two semi clusters are the same when:
     * (i) they have the same number of vertices,
     * (ii) all their vertices are the same
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
      if (this.size() < other.size()) {
        return -1;
      }
      if (this.size() > other.size()) {
        return 1;
      }
      if (other.vertices.containsAll(vertices)) {
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
      if (vertices != null) {
        result += vertices.hashCode();
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
      if (vertices == null) {
        if (other.vertices != null) {
          return false;
        }
      } else if (!vertices.equals(other.vertices)) {
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
      for (LongWritable v: this.vertices) {
        builder.append(v.toString());
        builder.append(" ");
      }
      builder.append(" | " + score + ", " + innerScore + ", "
          + boundaryScore + " ]");
      return builder.toString();
    }

    /**
     * Read fields.
     *
     * @param input Input to be read.
     * @throws IOException for IO.
     */
    @Override
    public final void readFields(final DataInput input) throws IOException {
      vertices = new HashSet<LongWritable>();
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        LongWritable e = new LongWritable();
        e.readFields(input);
        vertices.add(e);
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
      output.writeInt(size());
      for (LongWritable vertex: vertices) {
        vertex.write(output);
      }
      output.writeDouble(score);
      output.writeDouble(innerScore);
      output.writeDouble(boundaryScore);
    }
  }
}