package ml.grafos.okapi.cf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class CfLongIdTest {

  @Test
  public void testEmptySerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    
    CfLongId id = new CfLongId();
    id.write(output);
    
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    
    CfLongId idCopy = new CfLongId();
    idCopy.readFields(input);
    
    assertTrue(id.equals(idCopy));
  }
  
  @Test
  public void testSerialization() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10000);
    DataOutput output = new DataOutputStream(baos);
    
    CfLongId id = new CfLongId((byte)100, 200);
    id.write(output);
    
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(
        baos.toByteArray()));
    
    CfLongId idCopy = new CfLongId();
    idCopy.readFields(input);
    
    assertTrue(idCopy.getType()==100);
    assertTrue(idCopy.getId()==200);
    assertTrue(id.equals(idCopy));
  }
  
  @Test
  public void testCompare() {
    CfLongId id1 = new CfLongId((byte)100, 200);
    CfLongId id2 = new CfLongId((byte)100, 200);
    CfLongId id3 = new CfLongId((byte)101, 200);
    CfLongId id4 = new CfLongId((byte)100, 199);
    assertEquals(id1, id2);
    assertTrue(!id1.equals(id3));
    assertTrue(!id1.equals(null));
    assertTrue(id1.compareTo(id2)==0);
    assertTrue(id1.compareTo(id3)==-1);
    assertTrue(id1.compareTo(id4)==1);
  }
}
