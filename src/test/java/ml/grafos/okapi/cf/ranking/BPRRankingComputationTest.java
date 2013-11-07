package ml.grafos.okapi.cf.ranking;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class BPRRankingComputationTest {
	BPRRankingComputation bpr;
	
	@Before
	public void setUp() throws Exception {
		bpr = new BPRRankingComputation();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testFull() throws Exception{
		String[] graph = { 
				"1 -1 1",
				"2 -1 1",
				"3 -2 1",
				"4 -1 1",
				"4 -2 1",
				"5 -3 1",
		};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(BPRRankingComputation.class);
		conf.setEdgeInputFormatClass(LongLongIntInputFormat.class);
		conf.set("minItemId", "-3");
		conf.set("maxItemId", "-1");
		conf.set("iter", "1");
		conf.setVertexOutputFormatClass(LongDoubleArrayListLongArrayListOutputFormat.class);
		Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
		List<String> res = new LinkedList<String>();
		for (String string : results) {
			res.add(string);
			System.out.println(string);
		}
		Assert.assertEquals(8, res.size());
	}
	
	@Test
	public void testsampleRelevantAndIrrelevantEdges(){
		//lets setup vertex mock with one edge
		
		bpr.setMaxItemId(-1);
		bpr.setMinItemId(-1);
		BPRRankingComputation bprMock = spy(bpr);
		doNothing().when(bprMock).sendMessage(any(LongWritable.class), any(LongDoubleArrayListMessage.class));
		
		Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex = mock(Vertex.class);
		List<Edge<LongWritable, IntWritable>> edges = new LinkedList<Edge<LongWritable,IntWritable>>();
		edges.add(EdgeFactory.create(new LongWritable(-5), new IntWritable(1)));
		when(vertex.getId()).thenReturn(new LongWritable(1));
		when(vertex.getNumEdges()).thenReturn(1);
		when(vertex.getEdges()).thenReturn(edges);
		
		bprMock.sampleRelevantAndIrrelevantEdges(vertex);
		verify(bprMock).sendRequestForFactors(-5, 1, true);
		verify(bprMock).sendRequestForFactors(-1, 1, false);
	}
	
	@Test
	public void testgetRandomItemId(){
		bpr.setMinItemId(-1);
		bpr.setMaxItemId(-1);
		long relevant = bpr.getRandomItemId(new HashSet<Long>());
		Assert.assertEquals(-1, relevant);
		
		bpr.setMinItemId(-2);
		bpr.setMaxItemId(-1);
		HashSet<Long> hashSet = new HashSet<Long>();
		hashSet.add(-1l);
		Assert.assertEquals(-2l, bpr.getRandomItemId(hashSet));
	}

}
