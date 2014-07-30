package ml.grafos.okapi.clustering.kmeans;

import java.io.IOException;

import ml.grafos.okapi.common.data.ArrayListOfDoubleArrayListWritable;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.python.modules.math;

/**
 * 
 * The k-means clustering algorithm partitions <code>N</code> data points (observations) into <code>k</code> clusters.
 * The input consists of data points with a <code>pointID</code> and a vector of coordinates.
 * 
 * The algorithm is iterative and works as follows.
 * In the initialization phase, <code>k</code> clusters are chosen from the input points at random.
 * 
 * In each iteration:
 * 1. each data point is assigned to the cluster center which is closest to it, by means of euclidean distance
 * 2. new cluster centers are recomputed, by calculating the arithmetic mean of the assigned points
 * 
 * Convergence is reached when the positions of the cluster centers do not change.
 *
 * http://en.wikipedia.org/wiki/K-means_clustering
 * 
 */
public class KMeansClustering {

  /**
   * The prefix for the cluster center aggregators, used to store the cluster centers coordinates.
   * Each of them aggregates the vector coordinates of points assigned to the cluster center,
   * in order to compute the means as the coordinates of the new cluster centers.
   */
  public static String CENTER_AGGR_PREFIX = "center.aggr.prefix";

  /**
   * The prefix for the aggregators used to store the number of points 
   * assigned to each cluster center
   */
  public static String ASSIGNED_POINTS_PREFIX = "assigned.points.prefix";
  
  /** The initial centers aggregator*/
  public static String INITIAL_CENTERS = "kmeans.initial.centers";
  
  /** Maximum number of iterations */
  public static final String MAX_ITERATIONS = "kmeans.iterations";
  /** Default value for iterations */
  public static final int ITERATIONS_DEFAULT = 100;
  /** Number of cluster centers */
  public static final String CLUSTER_CENTERS_COUNT = "kmeans.cluster.centers.count";
  /** Default number of cluster centers */
  public static final int CLUSTER_CENTERS_COUNT_DEFAULT = 3;
  /** Dimensions of the input points*/
  public static final String DIMENSIONS = "kmeans.points.dimensions";
  /** Total number of input points*/
  public static final String POINTS_COUNT = "kmeans.points.count"; 
  /** Parameter that enables printing the final centers coordinates **/
  public static final String PRINT_FINAL_CENTERS = "kmeans.print.final.centers";
  /** False by default **/
  public static final boolean PRINT_FINAL_CENTERS_DEFAULT = false;

  public static class RandomCentersInitialization extends BasicComputation<
  LongWritable, KMeansVertexValue, NullWritable, NullWritable> {
	
	@Override
	public void compute(
			Vertex<LongWritable, KMeansVertexValue, NullWritable> vertex,
			Iterable<NullWritable> messages) throws IOException {
		ArrayListOfDoubleArrayListWritable value = new ArrayListOfDoubleArrayListWritable();
		value.add(vertex.getValue().getPointCoordinates());
		aggregate(INITIAL_CENTERS, value);
		}
  }
  
  public static class KMeansClusteringComputation extends BasicComputation
  			<LongWritable, KMeansVertexValue, NullWritable, NullWritable> {
	  private int clustersCount;
	  
	@Override
	public void preSuperstep() {
		clustersCount = getContext().getConfiguration()
				  .getInt(CLUSTER_CENTERS_COUNT, CLUSTER_CENTERS_COUNT_DEFAULT);
	}
	  
	@Override
	public void compute(
			Vertex<LongWritable, KMeansVertexValue, NullWritable> vertex,
			Iterable<NullWritable> messages) throws IOException {
		KMeansVertexValue currentValue = vertex.getValue();
		final DoubleArrayListWritable pointCoordinates = currentValue.getPointCoordinates();
		// read the cluster centers coordinates
		DoubleArrayListWritable[] clusterCenters = readClusterCenters(CENTER_AGGR_PREFIX);
		// find the closest center
		final int centerId = findClosestCenter(clusterCenters, currentValue.getPointCoordinates());
		// aggregate this point's coordinates to the cluster centers aggregator
		aggregate(CENTER_AGGR_PREFIX + "C_" + centerId, pointCoordinates);
		// increase the count of assigned points for this cluster center
		aggregate(ASSIGNED_POINTS_PREFIX + "C_" + centerId, new IntWritable(1));
		// set the cluster id in the vertex value
		vertex.getValue().setClusterId(new IntWritable(centerId));
	}

	private DoubleArrayListWritable[] readClusterCenters(String prefix) {
		DoubleArrayListWritable centers [] = new DoubleArrayListWritable[clustersCount];
		for ( int i = 0; i < clustersCount; i++ ) {
			centers[i] = getAggregatedValue(prefix + "C_" + i);
		}
		return centers;
	}

	/**
	 * finds the closest center to the given point
	 * by minimizing the Euclidean distance
	 * 
	 * @param clusterCenters
	 * @param value
	 * @return the index of the cluster center in the clusterCenters vector
	 */
	private int findClosestCenter(DoubleArrayListWritable[] clusterCenters,
			DoubleArrayListWritable value) {
		double minDistance = Double.MAX_VALUE;
		double distanceFromI;
		int clusterIndex = 0;
		for ( int i = 0; i < clusterCenters.length; i++ ) {
			distanceFromI = euclideanDistance(clusterCenters[i], value, 
					clusterCenters[i].size()); 
			if ( distanceFromI < minDistance ) {
				minDistance = distanceFromI;
				clusterIndex = i;
			}
		}
		return clusterIndex;
	}

	/**
	 * Calculates the Euclidean distance between two vectors of doubles
	 * 
	 * @param v1
	 * @param v2
	 * @param dim 
	 * @return
	 */
	private double euclideanDistance(DoubleArrayListWritable v1, DoubleArrayListWritable v2, int dim) {
		double distance = 0.0;
		for ( int i = 0; i < dim; i++ ) {
			distance += math.pow(v1.get(i).get() - v2.get(i).get(), 2);
		}
		return math.sqrt(distance);
	}
  }

  /**
   * The Master calculates the new cluster centers
   * and also checks for convergence
   */
  public static class KMeansMasterCompute extends DefaultMasterCompute {
	  private int maxIterations;
	  private DoubleArrayListWritable[] currentClusterCenters;
	  private int clustersCount;
	  private int dimensions;
	    
    @Override
    public final void initialize() throws InstantiationException,
        IllegalAccessException {
    	maxIterations = getContext().getConfiguration().getInt(MAX_ITERATIONS, 
    			ITERATIONS_DEFAULT);
    	clustersCount = getContext().getConfiguration().getInt(CLUSTER_CENTERS_COUNT, 
    			CLUSTER_CENTERS_COUNT_DEFAULT);
    	dimensions = getContext().getConfiguration().getInt(DIMENSIONS, 0);
    	currentClusterCenters = new DoubleArrayListWritable[clustersCount];
    	// register initial centers aggregator
    	registerAggregator(INITIAL_CENTERS, ArrayListOfDoubleArrayListWritableAggregator.class);
    	// register aggregators, one per center for the coordinates and
    	// one per center for counts of assigned elements
    	for ( int i = 0; i < clustersCount; i++ ) {
    		registerAggregator(CENTER_AGGR_PREFIX + "C_" + i, DoubleArrayListWritableAggregator.class);
    		registerAggregator(ASSIGNED_POINTS_PREFIX + "C_" + i, IntSumAggregator.class);
    	}
    }
    
    @Override
    public final void compute() {
	    long superstep = getSuperstep();
	    if ( superstep == 0 ) {
	    	setComputation(RandomCentersInitialization.class);
	    }
	    else {
	    	setComputation(KMeansClusteringComputation.class);
	    
		    if ( superstep == 1 ) {
		    	// initialize the centers aggregators
		    	ArrayListOfDoubleArrayListWritable initialCenters = getAggregatedValue(INITIAL_CENTERS);
		    	for ( int i = 0; i < clustersCount; i++ ) {
		    		setAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i, initialCenters.get(i));
		    		currentClusterCenters[i] = initialCenters.get(i);
		    	}
		    }
		    else {
			    // compute the new centers positions
		    	DoubleArrayListWritable[] newClusters = computeClusterCenters();		
			     //check for convergence
			    if ( (superstep > maxIterations) || (clusterPositionsDiff(currentClusterCenters, newClusters)) ) {
			    	
			    	// if enabled, print the final centers coordinates
			    	if ( getContext().getConfiguration().getBoolean(PRINT_FINAL_CENTERS, PRINT_FINAL_CENTERS_DEFAULT) ) {
			    		printFinalCentersCoordinates();
			    	}
			    	
			  	  	haltComputation();
			    }
			    else {
			  	  	// update the aggregators with the new cluster centers
			  	  	for ( int i = 0; i < clustersCount; i ++ ) {
			  	  		setAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i, newClusters[i]);
			  	  	}
			  	  	currentClusterCenters = newClusters;
			    } 
		    }
	    }
    }

	private DoubleArrayListWritable[] computeClusterCenters() {
		DoubleArrayListWritable[] newClusterCenters = new DoubleArrayListWritable[clustersCount];
		DoubleArrayListWritable clusterCoordinates;
		IntWritable assignedPoints;
		for ( int i = 0; i < clustersCount; i++ ) {
			clusterCoordinates = getAggregatedValue(CENTER_AGGR_PREFIX + "C_" + i);
			assignedPoints = getAggregatedValue(ASSIGNED_POINTS_PREFIX + "C_" + i);
			for ( int j = 0; j < clusterCoordinates.size(); j++ ) {
				clusterCoordinates.set(j, new DoubleWritable(
						clusterCoordinates.get(j).get() / assignedPoints.get()));
			}
			newClusterCenters[i] = clusterCoordinates;
		}
		return newClusterCenters;
	}

	private boolean clusterPositionsDiff(
			DoubleArrayListWritable[] currentClusterCenters,
			DoubleArrayListWritable[] newClusters) {
		final double E = 0.001f;
		double diff = 0;
		for ( int i = 0; i < clustersCount; i ++ ) {
			for ( int j = 0; j < dimensions; j ++ ) {
				diff += Math.abs(currentClusterCenters[i].get(j).get() - newClusters[i].get(j).get());
			}
		}
		if ( diff > E )
			return false;
		else
			return true;
	}
	
	private void printFinalCentersCoordinates() {
		System.out.println("Centers Coordinates: ");
    	for (int i = 0; i < clustersCount; i ++ ) {
    		System.out.print("cluster id " + i + ": ");
    		for ( int j = 0; j < currentClusterCenters[i].size(); j ++ ) {
    			System.out.print(currentClusterCenters[i].get(j) + " ");
    		}
    		System.out.println();
    	}		
	}

  }
}
