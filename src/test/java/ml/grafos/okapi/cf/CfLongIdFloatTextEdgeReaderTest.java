package ml.grafos.okapi.cf;

import static org.junit.Assert.*;

import java.io.IOException;

import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat.CfIdFloatTextEdgeReader;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class CfLongIdFloatTextEdgeReaderTest extends CfIdFloatTextEdgeReader {

  public CfLongIdFloatTextEdgeReaderTest() {
    new CfLongIdFloatTextInputFormat().super();
  }
  
  @Test
  public void test() throws IOException {
    Text inputLine1 = new Text("1 2 5.0");
    Text inputLine2 = new Text("1\t2\t5.000  ");
    
    CfLongId user = new CfLongId((byte)0, 1);
    CfLongId item = new CfLongId((byte)1, 2);
    FloatWritable rating = new FloatWritable(5f);
    
    String tokens[] = this.preprocessLine(inputLine1);
    assertEquals(user, this.getSourceVertexId(tokens));
    assertEquals(item, this.getTargetVertexId(tokens));
    assertEquals(rating, this.getValue(tokens));
    
    tokens = this.preprocessLine(inputLine2);
    assertEquals(user, this.getSourceVertexId(tokens));
    assertEquals(item, this.getTargetVertexId(tokens));
    assertEquals(rating, this.getValue(tokens));
  }

}
