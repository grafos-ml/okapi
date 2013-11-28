package ml.grafos.okapi.cf.sgd;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InMemoryVertexInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.FloatWritable;
import org.jblas.FloatMatrix;
import org.junit.Ignore;
import org.junit.Test;

public class SgdTest {

  @Test
  public void testUpdateValue() {
    float rating = 1f;
    float lambda = 0.01f;
    float gamma = 0.005f;
    float minRating = 0f;
    float maxRating = 5f;

    Sgd sgd = new Sgd();
    
    //v = (0.1, 0.2, 0.3)
    FloatMatrix v = new FloatMatrix(3, 1, new float[]{0.1f, 0.2f, 0.3f});
    //u = (0.2, 0.1, 0.4)
    FloatMatrix u = new FloatMatrix(3, 1, new float[]{0.2f, 0.1f, 0.4f});

    sgd.updateValue(v, u, rating, minRating, maxRating, lambda, gamma);

    assertArrayEquals(v.data, 
        new float[]{0.100835f, 0.20041f, 0.301665f}, 0.000001f);
  }

  /**
   * This simply tests whether the number of unique vertices in the graph is 
   * correct.
   * @throws Exception
   */
  @Test
  public void testEndToEnd() throws Exception {
    String[] graph = { 
        "1 1 1.0",
        "1 2 2.0",
        "2 1 3.0",
        "2 2 4.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(Sgd.InitUsersComputation.class);
    conf.setMasterComputeClass(Sgd.MasterCompute.class);
    conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
    conf.setFloat(Sgd.GAMMA, 0.005f);
    conf.setFloat(Sgd.LAMBDA, 0.01f);
    conf.setInt(Sgd.VECTOR_SIZE, 2);
    conf.setInt(Sgd.ITERATIONS, 4);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
    }
    Assert.assertEquals(4, res.size());
  }

  @Ignore
  public void testInMemoryRun() throws Exception {
    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(Sgd.InitUsersComputation.class);
    conf.setMasterComputeClass(Sgd.MasterCompute.class);
    conf.setVertexInputFormatClass(InMemoryVertexInputFormat.class);
    conf.setFloat(Sgd.GAMMA, 0.005f);
    conf.setFloat(Sgd.LAMBDA, 0.01f);
    conf.setInt(Sgd.VECTOR_SIZE, 2);
    conf.setInt(Sgd.ITERATIONS, 4);

    TestGraph<CfLongId, FloatMatrixWritable, FloatWritable> testGraph = 
        new TestGraph(conf);

    testGraph.addEdge(new CfLongId((byte)0, 1), new CfLongId((byte)1, 1), 
        new FloatWritable(1.0f));
    testGraph.addEdge(new CfLongId((byte)0, 1), new CfLongId((byte)1, 2), 
        new FloatWritable(2.0f));
    testGraph.addEdge(new CfLongId((byte)0, 2), new CfLongId((byte)1, 1), 
        new FloatWritable(3.0f));
    testGraph.addEdge(new CfLongId((byte)0, 2), new CfLongId((byte)1, 2), 
        new FloatWritable(4.0f));

    TestGraph<CfLongId, FloatMatrixWritable, FloatWritable> resultGraph = 
        InternalVertexRunner.run(conf, testGraph);

    Assert.assertNotNull(resultGraph.getVertex(new CfLongId((byte)0, 1)));
    Assert.assertNotNull(resultGraph.getVertex(new CfLongId((byte)1, 1)));  
  }
}
