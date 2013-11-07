package ml.grafos.okapi.cf.sgd;

import static org.junit.Assert.*;

import org.jblas.FloatMatrix;
import org.junit.Test;

public class SgdTest {
  
  Sgd sgd = new Sgd();

  @Test
  public void testUpdateValue() {
    float rating = 1f;
    float lambda = 0.01f;
    float gamma = 0.005f;
    
    //v = (0.1, 0.2, 0.3)
    FloatMatrix v = new FloatMatrix(3, 1, new float[]{0.1f, 0.2f, 0.3f});
    //u = (0.2, 0.1, 0.4)
    FloatMatrix u = new FloatMatrix(3, 1, new float[]{0.2f, 0.1f, 0.4f});
    
    sgd.updateValue(v, u, rating, lambda, gamma);
    
    assertArrayEquals(v.data, 
        new float[]{0.100835f, 0.20041f, 0.301665f}, 0.0000001f);
  }

}
