package ml.grafos.okapi.cf.svd;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;
import ml.grafos.okapi.cf.sgd.Sgd;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.jblas.FloatMatrix;
import org.junit.Test;

public class SvdppTest {
  
  @Test
  public void testUserUpdate() {
    float lambda = 0.01f;
    float gamma = 0.005f;
    float error = 1f;
    
    //user = (0.1, 0.2, 0.3)
    FloatMatrix user = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    //item = (0.2, 0.1, 0.4)
    FloatMatrix item = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    
    Svdpp.UserComputation comp = new Svdpp.UserComputation();
    comp.updateValue(user, item, error, gamma, lambda);
    
    assertArrayEquals(user.data, new float[] {0.100995f, 0.20049f, 0.301985f}, 
        0.000001f );
  }
  
  @Test
  public void testIncrementValue() {
    float lambda = 0.01f;
    float gamma = 0.005f;
    
    //value = (0.1, 0.2, 0.3)
    FloatMatrix value = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    //step = (0.2, 0.1, 0.4)
    FloatMatrix step = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    
    Svdpp.incrementValue(value, step, gamma, lambda);
    
    assertArrayEquals(value.data, new float[] {0.3f, 0.3f, 0.7f}, 0.0001f);
  }
  
  @Test
  public void testUpdateBaseline() {
    float baseline = 0.5f;
    float predictedRating = 4f;
    float observedRating = 3f; 
    float gamma = 0.005f;
    float lambda = 0.01f;
    
    float newBaseline = Svdpp.computeUpdatedBaseLine(baseline, predictedRating, 
        observedRating, gamma, lambda);
    
    assertEquals(newBaseline, 0.50475, 0.001f);
  }
  
  @Test
  public void testPredictRating() {
    float meanRating = 3;
    float userBaseline = 4;
    float itemBaseline = 2;
    float minRating = 0f;
    float maxRating = 5f;
    int numRatings = 10;
    //user = (0.1, 0.2, 0.3)
    FloatMatrix user = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    //item = (0.2, 0.1, 0.4)
    FloatMatrix item = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    //weights = (0.4, 0.6, 0.8)
    FloatMatrix weights = new FloatMatrix(1, 3, new float[]{0.4f, 0.6f, 0.8f});
    
    float prediction = Svdpp.computePredictedRating(meanRating, userBaseline, 
        itemBaseline, user, item, numRatings, weights, minRating, maxRating);
    
    assertEquals(prediction, 5.0f , 0.000001f);
    
    userBaseline = -2;
    prediction = Svdpp.computePredictedRating(meanRating, userBaseline, 
        itemBaseline, user, item, numRatings, weights, minRating, maxRating);

    assertEquals(prediction, 3.305464f , 0.000001f);
  }
  
  @Test
  public void testValueSerialization() throws IOException {
    float baseline = 0.5f;
    FloatMatrixWritable factors = 
        new FloatMatrixWritable(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    FloatMatrixWritable weight = 
        new FloatMatrixWritable(1, 3, new float[]{0.0f, Float.MAX_VALUE, 0.3f});
    Svdpp.SvdppValue value = new Svdpp.SvdppValue(baseline, factors, weight);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    value.write(output);

    Svdpp.SvdppValue valueCopy = new Svdpp.SvdppValue();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    valueCopy.readFields(input);
    
    assertEquals(value.getBaseline(), valueCopy.getBaseline(), 0.000001f);
    assertEquals(value.getFactors(), valueCopy.getFactors());
    assertEquals(value.getWeight(), valueCopy.getWeight());
  }

  @Test
  public void testEndtoEnd() throws Exception {
    String[] graph = { 
        "1 1 1.0",
        "1 2 2.0",
        "2 1 3.0",
        "2 2 4.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(Svdpp.InitUsersComputation.class);
    conf.setMasterComputeClass(Svdpp.MasterCompute.class);
    conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
    conf.setFloat(Svdpp.BIAS_LAMBDA, 0.005f);
    conf.setFloat(Svdpp.BIAS_GAMMA, 0.01f);
    conf.setFloat(Svdpp.FACTOR_LAMBDA, 0.005f);
    conf.setFloat(Svdpp.FACTOR_GAMMA, 0.01f);
    conf.setInt(Svdpp.VECTOR_SIZE, 2);
    conf.setInt(Svdpp.ITERATIONS, 4);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
    }
    Assert.assertEquals(4, res.size());
    
  }
}
