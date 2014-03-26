package ml.grafos.okapi.kmeans;

import static org.junit.Assert.*;

import java.util.Set;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

public class TestKMeansClustering {

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
     
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 7);
        conf.setMasterComputeClass(KMeansClustering.KMeansMasterCompute.class);
        conf.setComputationClass(KMeansClustering.RandomCentersInitialization.class);
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
     
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 3);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 8);
        conf.setMasterComputeClass(KMeansClustering.KMeansMasterCompute.class);
        conf.setComputationClass(KMeansClustering.RandomCentersInitialization.class);
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
        	if ( cluster.contains(1)) {
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
     
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.CLUSTER_CENTERS_COUNT, 2);
		conf.setInt(ArrayListOfDoubleArrayListWritableAggregator.POINTS_COUNT, 10);
        conf.setMasterComputeClass(KMeansClustering.KMeansMasterCompute.class);
        conf.setComputationClass(KMeansClustering.RandomCentersInitialization.class);
        conf.setVertexInputFormatClass(KMeansTextInputFormat.class);
        conf.setOutEdgesClass(NullOutEdges.class);
        conf.setVertexOutputFormatClass(KMeansTextOutputFormat.class);
        conf.setInt(KMeansClustering.CLUSTER_CENTERS_COUNT, 2);
        conf.setInt(KMeansClustering.DIMENSIONS, 2);
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

}
