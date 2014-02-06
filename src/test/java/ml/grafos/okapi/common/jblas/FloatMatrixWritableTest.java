/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    
    FloatMatrixWritable row = new FloatMatrixWritable(1, 2, 0.1f, 0.5f);
    assertTrue(row.equals(fmwCopy.getRow(0)));
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
