package ml.grafos.okapi.cf.ranking;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * @author User: linas
 *         Date: 11/25/13
 */
public class RandomRankingComputationTest {

    static List<String> res;
    static List<String> res2;

    @Before
    public void setUp() throws Exception {
        String[] graph = {
                "1 1 1",
                "2 1 1"
        };

        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(RandomRankingComputation.class);
        conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        res = new LinkedList<String>();
        for (String string : results) {
            res.add(string);
        }

        RandomRankingComputation.setDim(10);
        results = InternalVertexRunner.run(conf, null, graph);
        res2 = new LinkedList<String>();
        for (String string : results) {
            res2.add(string);
        }

    }

    @Test
    public void testFullComputation() throws Exception {
        Assert.assertEquals(3, res.size());
        Assert.assertTrue(res.get(0).startsWith("1 0\t["));
        Assert.assertTrue(res.get(1).startsWith("2 0\t["));
        Assert.assertTrue(res.get(2).startsWith("1 1\t["));
    }

    @Test
    public void testDimensionality() throws Exception {
        Assert.assertEquals(2, res.get(0).split(";").length);
        Assert.assertEquals(2, res.get(1).split(";").length);
        Assert.assertEquals(2, res.get(2).split(";").length);

        Assert.assertEquals(10, res2.get(0).split(";").length);
        Assert.assertEquals(10, res2.get(1).split(";").length);
        Assert.assertEquals(10, res2.get(2).split(";").length);
    }
}
