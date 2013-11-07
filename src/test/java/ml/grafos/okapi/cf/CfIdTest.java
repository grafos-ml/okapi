package ml.grafos.okapi.cf;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.junit.Test;

public class CfIdTest {

  @Test
  public void testEmptySerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    
    CfId id = new CfId();
    id.write(output);
    
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    
    CfId idCopy = new CfId();
    idCopy.readFields(input);
    
    assertTrue(id.equals(idCopy));
  }
  
  @Test
  public void testSerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    
    CfId id = new CfId(100, 200);
    id.write(output);
    
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    
    CfId idCopy = new CfId();
    idCopy.readFields(input);
    
    assertTrue(idCopy.getType()==100);
    assertTrue(idCopy.getId()==200);
    assertTrue(id.equals(idCopy));
  }
  
  @Test
  public void testCompare() {
    CfId id1 = new CfId(100, 200);
    CfId id2 = new CfId(100, 200);
    CfId id3 = new CfId(101, 200);
    CfId id4 = new CfId(100, 199);
    assertEquals(id1, id2);
    assertTrue(!id1.equals(id3));
    assertTrue(!id1.equals(null));
    assertTrue(id1.compareTo(id2)==0);
    assertTrue(id1.compareTo(id3)==-1);
    assertTrue(id1.compareTo(id4)==1);
  }
}
