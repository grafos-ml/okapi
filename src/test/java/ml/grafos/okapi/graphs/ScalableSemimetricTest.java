package ml.grafos.okapi.graphs;

import java.util.ArrayList;

import junit.framework.Assert;
import ml.grafos.okapi.io.formats.EdgesWithValuesVertexOutputFormat;
import ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class ScalableSemimetricTest {

	@Test
	public void testTwoSemimetric() throws Exception {
        String[] graph = new String[] {
        		"1	2	1.0",
        		"2	1	1.0",
        		"2	3	1.0",
        		"3	2	1.0",
        		"3	1	10.0",
        		"1	3	10.0",
        		"3	6	1.0",
        		"6	3	1.0",
        		"5	6	10.0",
        		"6	5	10.0",
        		"4	5	1.0",
        		"5	4	1.0",
        		"3	5	1.0",
        		"5	3	1.0",
        		"2	4	1.0",
        		"4	2	1.0"
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(ScalableSemimetric.SemimetricMasterCompute.class);
        conf.setComputationClass(ScalableSemimetric.PropagateId.class);
        conf.setEdgeInputFormatClass(LongDoubleBooleanEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        conf.setInt("semimetric.subsupersteps", 3);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing two semimetric...");
        for (String s: results) {
        	String[] tokens = s.split("[\t ]");
        	Assert.assertEquals(false, ((Integer.parseInt(tokens[0]) == 1) && (Integer.parseInt(tokens[1]) == 3)));
        	Assert.assertEquals(false, ((Integer.parseInt(tokens[0]) == 3) && (Integer.parseInt(tokens[1]) == 1)));
        	Assert.assertEquals(false, ((Integer.parseInt(tokens[0]) == 5) && (Integer.parseInt(tokens[1]) == 6)));
        	Assert.assertEquals(false, ((Integer.parseInt(tokens[0]) == 6) && (Integer.parseInt(tokens[1]) == 5)));
         	Assert.assertEquals(1.0, Double.parseDouble(tokens[2]));
        	System.out.println(s);
        }
        System.out.println();
    }
	
	@Test
	public void testAllMetric() throws Exception {
        String[] graph = new String[] {
        		"1	2	1.0",
        		"2	1	1.0",
        		"2	3	1.0",
        		"3	2	1.0",
        		"3	1	1.0",
        		"1	3	1.0",
        		"3	6	1.0",
        		"6	3	1.0",
        		"5	6	1.0",
        		"6	5	1.0",
        		"4	5	1.0",
        		"5	4	1.0",
        		"3	5	1.0",
        		"5	3	1.0",
        		"2	4	1.0",
        		"4	2	1.0"
                 };
	      	
        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(ScalableSemimetric.SemimetricMasterCompute.class);
        conf.setComputationClass(ScalableSemimetric.PropagateId.class);
        conf.setEdgeInputFormatClass(LongDoubleBooleanEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(EdgesWithValuesVertexOutputFormat.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        conf.setInt("semimetric.subsupersteps", 3);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        System.out.println("Testing all metric...");
        ArrayList<String> resultsList = new ArrayList<String>();
        for (String s: results) {
        	resultsList.add(s);
        }
        Assert.assertEquals(16, resultsList.size());
    }
}