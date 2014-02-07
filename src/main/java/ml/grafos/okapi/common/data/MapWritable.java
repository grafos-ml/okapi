package ml.grafos.okapi.common.data;

/**
 * Extends the MapWritable from Hadoop and adds extra functionality.
 * @author dl
 *
 */
public class MapWritable extends org.apache.hadoop.io.MapWritable {
  
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (Entry e : entrySet()) {
      s.append("("+e.getKey()+","+e.getValue()+")");
    }
    return s.toString();
  }
}
