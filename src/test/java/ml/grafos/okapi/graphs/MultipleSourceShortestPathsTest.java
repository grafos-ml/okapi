package ml.grafos.okapi.graphs;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import ml.grafos.okapi.common.Parameters;
import ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class MultipleSourceShortestPathsTest {

  @Test
  public void test() {
    String[] graph = { 
        "1 2 1.0",
        "2 1 1.0",
        "1 3 1.0",
        "3 1 1.0",
        "2 3 2.0",
        "3 2 2.0",
        "3 4 2.0",
        "4 3 2.0",
        "3 5 1.0",
        "5 3 1.0",
        "4 5 1.0",
        "5 4 1.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(MultipleSourceShortestPaths.InitSources.class);
    conf.setMasterComputeClass(MultipleSourceShortestPaths.MasterCompute.class);
    conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    conf.setFloat(MultipleSourceShortestPaths.SOURCES_FRACTION, 0.4f);
    conf.setLong(Parameters.RANDOM_SEED.getKey(), 0);
    Iterable<String> results;
    try {
      results = InternalVertexRunner.run(conf, null, graph);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occurred");
      return;
    }
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
      System.out.println(string);
    }
    Assert.assertEquals(5, res.size());
  }

}
