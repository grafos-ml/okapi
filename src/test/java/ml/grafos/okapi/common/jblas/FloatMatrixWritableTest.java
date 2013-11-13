package ml.grafos.okapi.common.jblas;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.jblas.FloatMatrix;
import org.junit.Before;
import org.junit.Test;

public class FloatMatrixWritableTest {

  private FloatMatrixWritable fmw;

  @Before
  public void setUp() throws Exception {
    fmw = new FloatMatrixWritable();
  }

  @Test
  public void testReadWrite() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    fmw = new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE);
    DataOutput output = new DataOutputStream(baos);
    fmw.write(output);
    FloatMatrixWritable fmwCopy = new FloatMatrixWritable();
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    fmwCopy.readFields(input);
    assertArrayEquals(fmw.toArray(), fmwCopy.toArray(), 0.001f);
    assertTrue(fmw.equals(fmwCopy));
  }

  @Test
  public void testFloatToByteAndBack() {
    float[] input = { 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE, Float.MIN_VALUE,
        Float.NEGATIVE_INFINITY };
    assertArrayEquals(input, fmw.toFloatArray(fmw.toByteArray(input)), 0.001f);
  }
  
  @Test
  public void testConstructor() {
    FloatMatrix fm = 
        new FloatMatrix(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE);
    FloatMatrixWritable fmw = 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE);
    FloatMatrixWritable fmwDup = new FloatMatrixWritable(fm); 
    Assert.assertEquals(fmwDup, fmw);
  }

}
