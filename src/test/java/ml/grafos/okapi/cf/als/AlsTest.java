package ml.grafos.okapi.cf.als;

import java.util.LinkedList;
import java.util.List;

import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.jblas.FloatMatrix;
import org.junit.Assert;
import org.junit.Test;

public class AlsTest {

  @Test
  public void testUpdateValue() {
    Als als = new Als();
    
    float lambda = 0.01f;

    //user = (0.1, 0.2, 0.3)
    //item1 = (0.2, 0.1, 0.4)
    //item2 = (0.1, 0.1, 0.1)
    //item3 = (0.3, 0.1, 0.3)
    //item4 = (0.1, 0.1, 0.3)
    //ratings: 1.0 2.0 3.0 4.0
    
    FloatMatrix user = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    FloatMatrix item1 = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    FloatMatrix item2 = new FloatMatrix(1, 3, new float[]{0.1f, 0.1f, 0.1f});
    FloatMatrix item3 = new FloatMatrix(1, 3, new float[]{0.3f, 0.1f, 0.3f});
    FloatMatrix item4 = new FloatMatrix(1, 3, new float[]{0.1f, 0.1f, 0.3f});
    
    FloatMatrix mat_M = new FloatMatrix(3,4);
    mat_M.putColumn(0, item1);
    mat_M.putColumn(1, item2);
    mat_M.putColumn(2, item3);
    mat_M.putColumn(3, item4);
    FloatMatrix mat_R = 
        new FloatMatrix(4,1, new float[]{1.0f, 2.0f, 3.0f, 4.0f});

    als.updateValue(user, mat_M, mat_R, lambda);
    
    Assert.assertArrayEquals(user.data, 
        new float[] {2.598314f, 4.297752f, 4.311797f}, 0.00001f);
  }

  @Test
  public void testEndToEnd() throws Exception {
    String[] graph = { 
        "1 1 1.0",
        "1 2 2.0",
        "2 1 3.0",
        "2 2 4.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(Als.InitUsersComputation.class);
    conf.setMasterComputeClass(Als.MasterCompute.class);
    conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
    conf.setFloat(Als.LAMBDA, 0.01f);
    conf.setInt(Als.VECTOR_SIZE, 2);
    conf.setInt(Als.ITERATIONS, 4);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
    }
    Assert.assertEquals(4, res.size()); 
  }
}
