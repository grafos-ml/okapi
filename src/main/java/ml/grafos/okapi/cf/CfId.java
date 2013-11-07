package ml.grafos.okapi.cf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class represents the ID of a node in a CF scenario. Nodes in this case
 * typically represent either users or items and are identified by a number or
 * a string (e.g. an item description). To avoid conflicts between user and item
 * ids, this class encapsulates the type of the node as well, not just the
 * identifier. The type is an integer and can by set and interpreted in an 
 * application-specific manner.
 * 
 * @author dl
 *
 */
public class CfId implements WritableComparable<CfId> {

  private int type;
  private long id;
  
  public CfId() {}
  
  public CfId(int type, long id) {
    this.type = type;
    this.id = id;
  }
  
  public int getType() {
    return type;
  }
  
  public long getId() {
    return id;
  }
  
  @Override
  public void readFields(DataInput input) throws IOException {
    type = input.readInt();
    id = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(type);
    output.writeLong(id);
  }

  /**
   * To objects of this class are the same only if both the type and the id
   * are the same.
   */
  @Override
  public int compareTo(CfId other) {
    if (type<other.type) {
      return -1;
    } else if (type>other.type){
      return 1;
    } else if (id<other.id) {
      return -1;
    } else if (id>other.id) {
      return 1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object cfid) {
    if (cfid==null) {
      return false;
    }
    CfId other = (CfId)cfid;
    if (type!=other.type) {
      return false;
    }
    if (id!=other.id) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns a string of the format:
   * <id><\space><type>
   */
  @Override
  public String toString() {
    return id+" "+type;
  }
}
