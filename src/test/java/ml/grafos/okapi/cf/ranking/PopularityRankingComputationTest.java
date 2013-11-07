package ml.grafos.okapi.cf.ranking;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PopularityRankingComputationTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testFullComputation() throws Exception {
		String[] graph = { 
				"1 -1 1",
				"2 -1 1"
		};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(PopularityRankingComputation.class);
		conf.setEdgeInputFormatClass(LongLongIntInputFormat.class);
		conf.setVertexOutputFormatClass(LongDoubleArrayListLongArrayListOutputFormat.class);
		Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
		List<String> res = new LinkedList<String>();
		for (String string : results) {
			res.add(string);
		}
		Assert.assertEquals(3, res.size());
	}
}
