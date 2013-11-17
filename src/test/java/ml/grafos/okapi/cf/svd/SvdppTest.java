package ml.grafos.okapi.cf.svd;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

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
  public void testItemUpdate() {
    float lambda = 0.01f;
    float gamma = 0.005f;
    float error = 1f;
    int numRatings = 10;
    
    //user = (0.1, 0.2, 0.3)
    FloatMatrix user = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    //item = (0.2, 0.1, 0.4)
    FloatMatrix item = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    //weights = (0.4, 0.6, 0.8)
    FloatMatrix weights = new FloatMatrix(1, 3, new float[]{0.4f, 0.6f, 0.8f});
    
    Svdpp.ItemComputation comp = new Svdpp.ItemComputation();
    comp.updateValue(item, user, weights, error, numRatings, gamma, lambda);
    
    assertArrayEquals(item.data, new float[] {
        0.201122455532034f, 0.101943683298051f, 0.402744911064067f}, 0.000001f);
  }
  
  @Test
  public void testItemWeightUpdate() {
    float lambda = 0.01f;
    float gamma = 0.005f;
    float error = 1f;
    int numRatings = 10;
    
    //weight = (0.1, 0.2, 0.3)
    FloatMatrix weight = new FloatMatrix(1, 3, new float[]{0.1f, 0.2f, 0.3f});
    //item = (0.2, 0.1, 0.4)
    FloatMatrix item = new FloatMatrix(1, 3, new float[]{0.2f, 0.1f, 0.4f});
    
    Svdpp.ItemComputation comp = new Svdpp.ItemComputation();
    comp.updateWeight(weight, item, error, numRatings, gamma, lambda);
    
    assertArrayEquals(weight.data, new float[] {
        0.100311227766017f, 0.200148113883008f, 0.300617455532034f}, 0.000001f);
  }
  
  @Test
  public void testUpdateBaseline() {
    float baseline = 0.5f;
    float predictedRating = 4f;
    float observedRating = 3f; 
    float gamma = 0.005f;
    float lambda = 0.01f;
    
    Svdpp svd = new Svdpp();
    float newBaseline = svd.computeUpdatedBaseLine(baseline, predictedRating, 
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
    
    Svdpp svd = new Svdpp();
    float prediction = svd.computePredictedRating(meanRating, userBaseline, 
        itemBaseline, user, item, numRatings, weights, minRating, maxRating);
    
    assertEquals(prediction, 5.0f , 0.000001f);
    
    userBaseline = -2;
    prediction = svd.computePredictedRating(meanRating, userBaseline, 
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
  public void testEndtoEnd() {
    fail("Not implemented yet!");
  }
}
