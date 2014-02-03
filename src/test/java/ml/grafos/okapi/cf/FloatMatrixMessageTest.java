/**
 * Copyright 2013 Grafos.ml
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
package ml.grafos.okapi.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.junit.Test;

public class FloatMatrixMessageTest {

  @Test
  public void testSerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    
    CfLongId id = new CfLongId((byte)9, 111);
    FloatMatrixWritable fmw = 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE);
    FloatMatrixMessage msg = new FloatMatrixMessage(id, fmw, 1.2f);
    
    msg.write(output);
    
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    
    FloatMatrixMessage msgCopy = new FloatMatrixMessage();
    msgCopy.readFields(input);
    
    assertTrue(msg.equals(msgCopy));
  }

  
  @Test
  public void testCompare() {
    FloatMatrixMessage msg1 = new FloatMatrixMessage(
        new CfLongId((byte)9,111), 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE), 
        1.2f);
    FloatMatrixMessage msg2 = new FloatMatrixMessage(
        new CfLongId((byte)9,111), 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE), 
        1.2f);
    FloatMatrixMessage msg3 = new FloatMatrixMessage(
        new CfLongId((byte)10,111), 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE), 
        1.2f);
    FloatMatrixMessage msg4 = new FloatMatrixMessage(
        new CfLongId((byte)9,111), 
        new FloatMatrixWritable(2, 2, 0.2f, 0.5f, Float.NaN, Float.MAX_VALUE), 
        1.2f);
    FloatMatrixMessage msg5 = new FloatMatrixMessage(
        new CfLongId((byte)9,111), 
        new FloatMatrixWritable(2, 2, 0.1f, 0.5f, Float.NaN, Float.MAX_VALUE), 
        1.3f);
    assertEquals(msg1, msg2);
    assertTrue(!msg1.equals(msg3));
    assertTrue(!msg1.equals(msg4));
    assertTrue(!msg1.equals(msg5));
  }
  
}
