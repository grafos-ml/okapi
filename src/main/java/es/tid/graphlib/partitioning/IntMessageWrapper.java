package es.tid.graphlib.partitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/** This class provides the wrapper for the sending message. */
public class IntMessageWrapper
    implements WritableComparable<IntMessageWrapper> {
  /** Message sender vertex Id */
  private IntWritable sourceId;
  /** Message with data */
  private IntWritable message;

  // TODO SHOULD BE STATIC RIGHT?
  // Should be actually removed!!!
  /** Configuration */
  private ImmutableClassesGiraphConfiguration<IntWritable, ?, ?> conf;

  /** Constructor */
  public IntMessageWrapper() {
  }

  /**
   * Constructor.
   * @param sourceId Vertex Source Id
   * @param message Message
   */
  public IntMessageWrapper(IntWritable sourceId, IntWritable message) {
    this.sourceId = sourceId;
    this.message = message;
  }

  /**
   * Return Vertex Source Id
   *
   * @return sourceId Message sender vertex Id
   */
  public IntWritable getSourceId() {
    return sourceId;
  }

  public void setSourceId(IntWritable sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * Return Message data
   *
   * @return message message to be returned
   */
  public IntWritable getMessage() {
    return message;
  }

  /**
   * Store message to this object
   *
   * @param message Message to be stored
   */
  public void setMessage(IntWritable message) {
    this.message = message;
  }
  /**
   * Get Configuration
   *
   * @return conf Configuration
   */
  public ImmutableClassesGiraphConfiguration<IntWritable, ?, ?> getConf() {
    return conf;
  }

  /**
   * Set Configuration
   *
   * @param conf Configuration to be stored
   */
  public void setConf(
    ImmutableClassesGiraphConfiguration<IntWritable, ?, ?> conf) {
    this.conf = conf;
  }

  /**
   * Read Fields
   *
   * @param input Input
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    sourceId = new IntWritable();
    toString();
    sourceId.readFields(input);
    message = new IntWritable();
    message.readFields(input);
  }

  /**
   * Write Fields
   *
   * @param output Output
   */
  @Override
  public void write(DataOutput output) throws IOException {

    if (sourceId == null) {
      throw new IllegalStateException("write: Null destination vertex index");
    }
    sourceId.write(output);
    message.write(output);
  }

  /**
   * Return Message to the form of a String
   *
   * @return String object
   */
  @Override
  public String toString() {
    return "MessageWrapper{" +
      ", sourceId=" + sourceId +
      ", message=" + message +
      '}';
  }

  /**
   * Check if object is equal to message
   *
   * @param o Object to be checked
   *
   * @return boolean value
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IntMessageWrapper that = (IntMessageWrapper) o;

    if (message != null ? !message.equals(that.message) :
        that.message != null) {
      return false;
    }
    if (sourceId != null ? !sourceId.equals(that.sourceId) :
        that.sourceId != null) {
      return false;
    }
    return true;
  }

  /**
   * CompareTo method
   *
   * @param wrapper WRapper to be compared to
   *
   * @return 0 if equal
   */
  @Override
  public int compareTo(IntMessageWrapper wrapper) {

    if (this == wrapper) {
      return 0;
    }
    if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) {
      return this.message.compareTo(wrapper.getMessage());
    } else {
      return this.sourceId.compareTo(wrapper.getSourceId());
    }
  }
}
