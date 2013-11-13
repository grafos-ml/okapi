package ml.grafos.okapi.cf;

import org.apache.hadoop.io.WritableComparable;

/**
 * Classes that implement this interface represent the ID of a node in a 
 * user-item graph. Nodes in this case typically represent either users or items 
 * and are identified by a number or a string (e.g. an item description). To 
 * avoid conflicts between user and item ids, this interface defines the type 
 * of the node as well, not just the identifier. The type is a byte value and 
 * can by set and interpreted in an application-specific manner.
 * 
 * @author dl
 *
 * @param <T>
 */
public interface CfId<T> extends WritableComparable<CfId<T>> {
  /**
   * Returns the type of the node.
   * @return
   */
  public byte getType();
  
  /**
   * Returns the identifier of the node.
   * @return
   */
  public T getId();
}
