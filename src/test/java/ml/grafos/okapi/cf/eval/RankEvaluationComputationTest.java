package ml.grafos.okapi.cf.eval;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RankEvaluationComputationTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testFullComputation() throws Exception {
		String[] graph = { 
				"0	0.0",
				"1	1,0,0	-1,-2",
				"2	2,0,0	-1",
				"-1	1,1,1	",
				"-2	0.5,1,1	",
				"-3	0,0,0	",
				"-4	0,0,0	"};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(RankEvaluationComputation.class);
		conf.setVertexInputFormatClass(LongArrayBooleanInputFormat.class);
		conf.setVertexOutputFormatClass(DoubleOutputFormat.class);
		conf.set("minItemId", "-4");
		conf.set("maxItemId", "-1");
		conf.set("numberSamples", "1");
		conf.set("k", "2");
		Iterable<String> results = InternalVertexRunner.run(conf, graph);

		int cnt = 0;
		for (String line : results) {
			assertEquals((1+0.5)/2.0, Double.parseDouble(line), 0.01);
			cnt += 1;
		}
		assertEquals(1,	cnt); //number of output
		
	}


	private static Map<Integer, Integer> parseResults(Iterable<String> results) {
		Map<Integer, Integer> values = new HashMap<Integer, Integer>();
		for (String line : results) {
			String[] tokens = line.split("\\s+");
			int id = Integer.valueOf(tokens[0]);
			int value = Integer.valueOf(tokens[1]);
			values.put(id, value);
		}
		return values;
	}
}
