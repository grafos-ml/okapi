package ml.grafos.okapi.cf.ranking;

import junit.framework.Assert;
import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class PopularityRankingComputationTest {

	@Test
	public void testFullComputation() throws Exception {
		String[] graph = { 
				"1 1 1",
				"2 1 1"
		};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(PopularityRankingComputation.class);
        conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
		List<String> res = new LinkedList<String>();
		for (String string : results) {
			res.add(string);
            //System.out.println(string);
        }
		Assert.assertEquals(3, res.size());
        Assert.assertTrue(res.contains("1 0\t[1.000000]"));
        Assert.assertTrue(res.contains("2 0\t[1.000000]"));
        Assert.assertTrue(res.contains("1 1\t[2.000000]"));
	}
}
