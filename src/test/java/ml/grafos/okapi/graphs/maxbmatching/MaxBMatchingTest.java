package ml.grafos.okapi.graphs.maxbmatching;

import static org.junit.Assert.*;

import java.util.Map;

import ml.grafos.okapi.graphs.maxbmatching.MBMEdgeValue.State;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.python.google.common.collect.Maps;

import com.google.common.base.Predicate;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.primitives.Longs;

public class MaxBMatchingTest {
    private static final Logger LOG = Logger.getLogger(MaxBMatchingTest.class);

    @Test
    public void test1() throws Exception {
        String[] graph = new String[] { 
                "1\t1\t2\t3.0\t3\t1.0",
                "2\t2\t1\t3.0\t4\t1.0\t5\t1.0",
                "3\t1\t1\t1.0\t5\t3.0",
                "4\t1\t2\t1.0\t5\t2.0",
                "5\t3\t2\t1.0\t3\t3.0\t4\t2.0"};
        
        /* output, removes 2 edges (1-3) (2-4)
         * 5   0   4   2.0 INCLUDED    2   1.0 INCLUDED    3   3.0 INCLUDED
         * 2   0   1   3.0 INCLUDED    5   1.0 INCLUDED
         * 1   0   2   3.0 INCLUDED
         * 3   0   5   3.0 INCLUDED
         * 4   0   5   2.0 INCLUDED
         */

        // run to check results correctness
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(MaxBMatching.class);
        conf.setOutEdgesClass(HashMapEdges.class);
        conf.setVertexInputFormatClass(MBMTextInputFormat.class);
        conf.setVertexOutputFormatClass(MBMTextOutputFormat.class);

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf, graph);
        Table<Long, Integer, Map<Long, MBMEdgeValue>> subgraph = parseResults(results);

        assertEquals(5, subgraph.size()); // five vertices
        
        // vertex 5
        assertEquals(1, subgraph.row(5L).size()); // only one capacity
        assertEquals(0, subgraph.row(5L).keySet().iterator().next().intValue()); // capacity is zero
        assertEquals(3, subgraph.get(5L, 0).size()); // three edges
        assertTrue(subgraph.get(5L, 0).keySet().containsAll(Longs.asList(4L, 2L, 3L))); // edges to these vertices
        Iterables.all(subgraph.get(5L, 0).values(), new Predicate<MBMEdgeValue>() {
            @Override
            public boolean apply(MBMEdgeValue edgeValue) {
                return edgeValue.getState() == State.INCLUDED;
            }
        }); // all edges are included
        
        // vertex 2
        assertEquals(0, subgraph.row(2L).keySet().iterator().next().intValue()); // capacity is zero
        assertTrue(subgraph.get(2L, 0).keySet().containsAll(Longs.asList(1L, 5L))); // edges to these vertices
        assertEquals(3.0, subgraph.get(2L, 0).get(1L).getWeight(), Double.MIN_VALUE); // edge weight to 1 is 3.0
        Iterables.all(subgraph.get(2L, 0).values(), new Predicate<MBMEdgeValue>() {
            @Override
            public boolean apply(MBMEdgeValue edgeValue) {
                return edgeValue.getState() == State.INCLUDED;
            }
        }); // all edges are included
    }

    private Table<Long, Integer, Map<Long, MBMEdgeValue>> parseResults(Iterable<String> results) {
        Table<Long, Integer, Map<Long, MBMEdgeValue>> graph = HashBasedTable.create();
        for (String result : results) {
            if (LOG.isDebugEnabled())
                LOG.debug(result);
            String[] parts = result.split("\t");
            long vertex = Long.parseLong(parts[0]);
            int capacity = Integer.parseInt(parts[1]);
            Map<Long, MBMEdgeValue> edges = Maps.newHashMap();

            for (int i = 2; i < parts.length; i += 3) {
                long targetVID = Long.parseLong(parts[i]);
                double weight = Double.parseDouble(parts[i + 1]);
                State state = State.valueOf(parts[i + 2]);
                MBMEdgeValue edgeValue = new MBMEdgeValue(weight, state);
                edges.put(targetVID, edgeValue);
            }
            graph.put(vertex, capacity, edges);
        }
        return graph;
    }
}
