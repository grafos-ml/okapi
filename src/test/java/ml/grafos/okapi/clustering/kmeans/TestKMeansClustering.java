package ml.grafos.okapi.clustering.kmeans;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import ml.grafos.okapi.clustering.kmeans.ArrayListOfDoubleArrayListWritableAggregator;
import ml.grafos.okapi.clustering.kmeans.KMeansClustering;
import ml.grafos.okapi.clustering.kmeans.KMeansTextInputFormat;
import ml.grafos.okapi.clustering.kmeans.KMeansTextOutputFormat;
import ml.grafos.okapi.clustering.kmeans.KMeansClustering.KMeansClusteringComputation;
import ml.grafos.okapi.common.data.ArrayListOfDoubleArrayListWritable;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import ml.grafos.okapi.common.graph.NullOutEdges;

import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

public class TestKMeansClustering {

	private static ArrayListOfDoubleArrayListWritable initial_centers = new ArrayListOfDoubleArrayListWritable();
	
	@Test
	public void test1() throws Exception {
        String[] graph = new String[] {
        		"1,1.0	1.0",
        		"2,1.5	2.0",
        		"3,3.0	4.0",
        		"4,5.0	7.0",
        		"5,3.5	5.0",
        		"6,4.5	5.0",
        		"7,3.5	4.5"
                 };
     
        /** set the initial centers **/
	     
        initial_centers.clear();
        
      	// create the centers points
      	DoubleArrayListWritable c1 = new DoubleArrayListWritable();
      	c1.add(new DoubleWritable(1.0));
      	c1.add(new DoubleWritable(1.0));
      	DoubleArrayListWritable c2 = new DoubleArrayListWritable();
      	c2.add(new DoubleWritable(1.5));
      	c2.add(new DoubleWritable(2.0));
      	
      	// add them to the array list
      	initial_centers.add(c1);
      	initial_centers.add(c2);
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 7);
        conf.setMasterComputeClass(KMeansTestMasterCompute.class);
        conf.setComputationClass(NoOpComputation.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
        conf.setInt(KMeansClustering.POINTS_COUNT, 7);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(2, clusterIDs.size());
        
        Set<Integer> clusterOne = clusters.get(0);
        Set<Integer> clusterTwo = clusters.get(1);
        if ( (clusterOne.size() == 2) && (clusterTwo.size() == 5)) {
        	assertTrue(clusterOne.contains(1));
        	assertTrue(clusterOne.contains(2));
            assertEquals(5, clusterTwo.size());
            assertTrue(clusterTwo.contains(3));
            assertTrue(clusterTwo.contains(4));
            assertTrue(clusterTwo.contains(5));
            assertTrue(clusterTwo.contains(6));
            assertTrue(clusterTwo.contains(7));
        }
        else  if ( (clusterOne.size() == 5) && (clusterTwo.size() == 2)) {
        	assertTrue(clusterTwo.contains(1));
        	assertTrue(clusterTwo.contains(2));
            assertEquals(5, clusterOne.size());
            assertTrue(clusterOne.contains(3));
            assertTrue(clusterOne.contains(4));
            assertTrue(clusterOne.contains(5));
            assertTrue(clusterOne.contains(6));
            assertTrue(clusterOne.contains(7));
        }
        else {
        	fail("Wrong cluster sizes");
        }
    }
	
	@Test
	public void test2() throws Exception {
        String[] graph = new String[] {
        		"1,2.0	10.0",
        		"2,2.0	5.0",
        		"3,8.0	4.0",
        		"4,5.0	8.0",
        		"5,7.0	5.0",
        		"6,6.0	4.0",
        		"7,1.0	2.0",
        		"8,4.0	9.0"
                 };
     
        /** set the initial centers **/
      	
        initial_centers.clear();
        
      	// create the centers points
      	DoubleArrayListWritable c1 = new DoubleArrayListWritable();
      	c1.add(new DoubleWritable(2.0));
      	c1.add(new DoubleWritable(10.0));
      	DoubleArrayListWritable c2 = new DoubleArrayListWritable();
      	c2.add(new DoubleWritable(2.0));
      	c2.add(new DoubleWritable(5.0));
      	DoubleArrayListWritable c3 = new DoubleArrayListWritable();
      	c3.add(new DoubleWritable(8.0));
      	c3.add(new DoubleWritable(4.0));
      	
      	// add them to the array list
      	initial_centers.add(c1);
      	initial_centers.add(c2);
      	initial_centers.add(c3);
      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 3);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 8);
        conf.setMasterComputeClass(KMeansTestMasterCompute.class);
        conf.setComputationClass(NoOpComputation.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
        conf.setInt(KMeansClustering.POINTS_COUNT, 8);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(3, clusterIDs.size());
        
        for ( int i = 0; i < clusterIDs.size(); i++ ) {
        	Set<Integer> cluster = clusters.get(i);
        	if ( cluster.contains(1) ) {
        		assertEquals(3, cluster.size());
        		assertTrue(cluster.contains(4));
        		assertTrue(cluster.contains(8));
        	}
        	else if ( cluster.contains(3) ) {
        		assertEquals(3, cluster.size());
        		assertTrue(cluster.contains(5));
        		assertTrue(cluster.contains(6));
        	}
        	else if ( cluster.contains(2) ) {
        		assertEquals(2, cluster.size());
        		assertTrue(cluster.contains(7));
        	}
        	else {
        		fail("Wrong clusters computed");
        	}
        }
    }
	
	@Test
	public void test3() throws Exception {
        String[] graph = new String[] {
        		"1,-4.31568	-0.396959	-6.29507",
        		"2,-4.56112	-1.74917	-4.57874",  
        		"3,4.54508	0.102845	6.35385",
        		"4,4.87746	-0.832591	7.06942",
        		"5,-5.91254	-0.278006	-4.25934",  
        		"6,6.95139	0.120139	4.89531",
        		"7,-6.28538	-0.88527	-4.74988",  
        		"8,-6.84791	0.887664	-4.91919",
        		"9,7.47117	1.67911	6.02221",
        		"10,-4.78011	1.2099	-4.55519"
                 };
     
        /** set the initial centers **/
      	
        initial_centers.clear();
        
      	// create the centers points
      	DoubleArrayListWritable c1 = new DoubleArrayListWritable();
      	c1.add(new DoubleWritable(-4.31568));
      	c1.add(new DoubleWritable(-0.396959));
      	c1.add(new DoubleWritable(-6.29507));
      	DoubleArrayListWritable c2 = new DoubleArrayListWritable();
      	c2.add(new DoubleWritable(-4.56112));
      	c2.add(new DoubleWritable(-1.74917));
      	c2.add(new DoubleWritable(-4.57874));
      	
      	// add them to the array list
      	initial_centers.add(c1);
      	initial_centers.add(c2);
      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 10);
        conf.setMasterComputeClass(KMeansTestMasterCompute.class);
        conf.setComputationClass(NoOpComputation.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        conf.setInt(KMeansClustering.DIMENSIONS, 3);
        conf.setInt(KMeansClustering.POINTS_COUNT, 10);
        conf.setBoolean(KMeansClustering.PRINT_FINAL_CENTERS, true);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(2, clusterIDs.size());
    }

    private SetMultimap<Integer,Integer> parseResults(
            Iterable<String> results) {
        SetMultimap<Integer,Integer> clusters = HashMultimap.create();
        for (String result : results) {
            Iterable<String> parts = Splitter.on(',').split(result);
            int point = Integer.parseInt(Iterables.get(parts, 0));
            int cluster = Integer.parseInt(Iterables.get(parts, 1));
            clusters.put(cluster, point);
        }
        return clusters;
    }

    /**
     * This test data was generated using Stratosphere's KMeansDataGenerator.
     * http://stratosphere.eu/
     */
    @Test
	public void test4() throws Exception {
        String[] graph = new String[] {
        		"1,-3.78	-42.01",
				"2,-45.96	30.67",
				"3,56.37	-46.62",
				"4,8.78	-37.95",
				"5,-26.95	43.10",
				"6,37.87	-51.30",
				"7,-2.61	-30.43",
				"8,-23.33	26.23",
				"9,38.19	-36.27",
				"10,-13.63	-42.26",
				"11,-36.57	32.63",
				"12,50.65	-52.40",
				"13,-5.76	-51.83",
				"14,-34.43	42.66",
				"15,40.35	-47.14",
				"16,-23.40	-48.70",
				"17,-29.58	17.77",
				"18,43.08	-61.96",
				"19,9.06	-49.26",
				"20,-20.13	44.16",
				"21,41.62	-45.84",
				"22,5.23	-41.20",
				"23,-23.00	38.15",
				"24,44.55	-51.50",
				"25,-15.63	-26.81",
				"26,-24.33	22.63",
				"27,52.51	-54.75",
				"28,-0.04	-39.69",
				"29,-32.92	43.87",
				"30,47.99	-36.93",
				"31,-7.34	-57.90",
				"32,-36.17	34.74",
				"33,51.52	-41.83",
				"34,-21.91	-49.01",
				"35,-46.68	46.04",
				"36,48.52	-43.67",
				"37,-0.20	-36.62",
				"38,-27.71	35.12",
				"39,41.29	-42.00",
				"40,-9.17	-43.28",
				"41,-41.16	50.66",
				"42,49.63	-45.28",
				"43,-8.10	-29.83",
				"44,-49.38	38.57",
				"45,35.38	-34.90",
				"46,-6.51	-55.58",
				"47,-38.17	40.21",
				"48,47.47	-45.95",
				"49,-17.66	-51.12",
				"50,-32.60	41.13",
				"51,40.68	-49.10",
				"52,-10.31	-40.69",
				"53,-22.05	42.91",
				"54,51.16	-47.58",
				"55,-12.42	-57.29",
				"56,-17.72	39.90",
				"57,44.57	-41.75",
				"58,3.14	-35.46",
				"59,-53.73	32.84",
				"60,53.16	-50.16"
                 };
        
        /** set the initial centers **/
      	
        initial_centers.clear();
        
      	// create the centers points
      	DoubleArrayListWritable c1 = new DoubleArrayListWritable();
      	c1.add(new DoubleWritable(-3.78));
      	c1.add(new DoubleWritable(-42.01));
      	DoubleArrayListWritable c2 = new DoubleArrayListWritable();
      	c2.add(new DoubleWritable(-36.57));
      	c2.add(new DoubleWritable(32.63));
      	DoubleArrayListWritable c3 = new DoubleArrayListWritable();
      	c3.add(new DoubleWritable(50.65));
      	c3.add(new DoubleWritable(-52.40));
      	
      	// add them to the array list
      	initial_centers.add(c1);
      	initial_centers.add(c2);
      	initial_centers.add(c3);

        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 3);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 60);
        conf.setMasterComputeClass(KMeansTestMasterCompute.class);
        conf.setComputationClass(NoOpComputation.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
        conf.setInt(KMeansClustering.POINTS_COUNT, 60);
        conf.setBoolean(KMeansClustering.PRINT_FINAL_CENTERS, true);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(3, clusterIDs.size());

        for ( int i = 0; i < clusterIDs.size(); i++ ) {
        	Set<Integer> cluster = clusters.get(i);
        	if ( cluster.contains(1) ) {
        		assertTrue(cluster.contains(7));
        		assertTrue(cluster.contains(40));
        		assertTrue(cluster.contains(46));
        	}
        	if ( cluster.contains(2) ) {
        		assertTrue(cluster.contains(11));
        		assertTrue(cluster.contains(32));
        		assertTrue(cluster.contains(35));
        		}
        	if ( cluster.contains(3) ) {
        		assertTrue(cluster.contains(12));
        		assertTrue(cluster.contains(15));
        		assertTrue(cluster.contains(21));
        		}
        }
    }

    /**
     * This test data was generated using Stratosphere's KMeansDataGenerator.
     * http://stratosphere.eu/
     */
    @Test
	public void test5() throws Exception {
    	String[] graph = new String[] {
    			"1,-3.78	-42.01",
				"2,-45.96	30.67",
				"3,56.37	-46.62",
				"4,8.78	-37.95",
				"5,-26.95	43.10",
				"6,37.87	-51.30",
				"7,-2.61	-30.43",
				"8,-23.33	26.23",
				"9,38.19	-36.27",
				"10,-13.63	-42.26"
				};
    	
    	/** set the initial centers **/
      	
        initial_centers.clear();
        
      	// create the centers points
      	DoubleArrayListWritable c1 = new DoubleArrayListWritable();
      	c1.add(new DoubleWritable(-3.78));
      	c1.add(new DoubleWritable(-42.01));
      	DoubleArrayListWritable c2 = new DoubleArrayListWritable();
      	c2.add(new DoubleWritable(-26.95));
      	c2.add(new DoubleWritable(43.10));
      	DoubleArrayListWritable c3 = new DoubleArrayListWritable();
      	c3.add(new DoubleWritable(56.37));
      	c3.add(new DoubleWritable(-46.62));
      	
      	// add them to the array list
      	initial_centers.add(c1);
      	initial_centers.add(c2);
      	initial_centers.add(c3);

        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 3);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 10);
        conf.setMasterComputeClass(KMeansTestMasterCompute.class);
        conf.setComputationClass(NoOpComputation.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 3);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
        conf.setInt(KMeansClustering.POINTS_COUNT, 10);
        conf.setBoolean(KMeansClustering.PRINT_FINAL_CENTERS, true);


        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        SetMultimap<Integer,Integer> clusters = parseResults(results);

        Set<Integer> clusterIDs = clusters.keySet();
        assertEquals(3, clusterIDs.size());

        for ( int i = 0; i < clusterIDs.size(); i++ ) {
        	Set<Integer> cluster = clusters.get(i);
        	if ( cluster.contains(1) ) {
        		assertTrue(cluster.contains(7));
        		assertTrue(cluster.contains(10));
        		}
        	if ( cluster.contains(2) ) {
        		assertTrue(cluster.contains(5));
        		assertTrue(cluster.contains(8));
        		}
        	if ( cluster.contains(3) ) {
        		assertTrue(cluster.contains(9));
        		assertTrue(cluster.contains(6));
        		}
        }
    }
    
    /** 
     * 
     * Special Master implementation only used for testing.
     * It is basically a copy of the KMeansMasterCompute class
     * that uses hard-code initial centroids, instead of 
     * using the random initialization. 
     *
     */
    public static class KMeansTestMasterCompute extends DefaultMasterCompute {
  	  private int maxIterations;
  	  private DoubleArrayListWritable[] currentClusterCenters;
  	  private int clustersCount;
  	  private int dimensions;
  	    
      @Override
      public final void initialize() throws InstantiationException,
          IllegalAccessException {
      	maxIterations = getContext().getConfiguration().getInt(KMeansClustering.MAX_ITERATIONS, 
      			KMeansClustering.ITERATIONS_DEFAULT);
      	clustersCount = getContext().getConfiguration().getInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 
      			KMeansClustering.CLUSTER_CENTERS_COUNT_DEFAULT);
      	dimensions = getContext().getConfiguration().getInt(KMeansClustering.DIMENSIONS, 0);
      	currentClusterCenters = new DoubleArrayListWritable[clustersCount];
      	// register initial centers aggregator
      	registerAggregator(KMeansClustering.INITIAL_CENTERS, ArrayListOfDoubleArrayListWritableAggregator.class);
      	// register aggregators, one per center for the coordinates and
      	// one per center for counts of assigned elements
      	for ( int i = 0; i < clustersCount; i++ ) {
      		registerAggregator(KMeansClustering.CENTER_AGGR_PREFIX + "C_" + i, DoubleArrayListWritableAggregator.class);
      		registerAggregator(KMeansClustering.ASSIGNED_POINTS_PREFIX + "C_" + i, IntSumAggregator.class);
      	}
      }
      
      @Override
      public final void compute() {
  	    long superstep = getSuperstep();

  	    if ( superstep == 0 ) {
  	      	setComputation(NoOpComputation.class);
  	    }
  	    else {
  	    	setComputation(KMeansClusteringComputation.class);

		    if ( superstep == 1 ) {
		    	// initialize the centers aggregators
		    	ArrayListOfDoubleArrayListWritable initialCenters = initial_centers;
		    	for ( int i = 0; i < clustersCount; i++ ) {
		    		setAggregatedValue(KMeansClustering.CENTER_AGGR_PREFIX + "C_" + i, initialCenters.get(i));
		    		currentClusterCenters[i] = initialCenters.get(i);
		    	}
		    }
		    else {
			    // compute the new centers positions
		    	DoubleArrayListWritable[] newClusters = computeClusterCenters();		
			     //check for convergence
			    if ( (superstep > maxIterations) || (clusterPositionsDiff(currentClusterCenters, newClusters)) ) {
			    	
			    	// if enabled, print the final centers coordinates
			    	if ( getContext().getConfiguration().getBoolean(KMeansClustering.PRINT_FINAL_CENTERS, 
			    			KMeansClustering.PRINT_FINAL_CENTERS_DEFAULT) ) {
			    		printFinalCentersCoordinates();
			    	}
			    	
			  	  	haltComputation();
			    }
			    else {
			  	  	// update the aggregators with the new cluster centers
			  	  	for ( int i = 0; i < clustersCount; i ++ ) {
			  	  		setAggregatedValue(KMeansClustering.CENTER_AGGR_PREFIX + "C_" + i, newClusters[i]);
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
  			clusterCoordinates = getAggregatedValue(KMeansClustering.CENTER_AGGR_PREFIX + "C_" + i);
  			assignedPoints = getAggregatedValue(KMeansClustering.ASSIGNED_POINTS_PREFIX + "C_" + i);
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
    
    public static class NoOpComputation extends BasicComputation<
    LongWritable, KMeansVertexValue, NullWritable, NullWritable> {
  	
	  	@Override
	  	public void compute(
	  			Vertex<LongWritable, KMeansVertexValue, NullWritable> vertex,
	  			Iterable<NullWritable> messages) throws IOException {
	
	  		// do nothing
	  	}
    }
    
}