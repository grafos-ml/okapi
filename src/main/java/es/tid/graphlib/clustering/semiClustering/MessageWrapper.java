package es.tid.graphlib.clustering.semiClustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.clustering.semiClustering.SemiClusteringMessageWrapper.SemiClusterTreeSetWritable;

/** This class provides the wrapper for the sending message.*/
public class MessageWrapper implements WritableComparable<MessageWrapper> {
  /** Message sender vertex Id */
  private LongWritable sourceId;
  /** Message with data */
  private SemiClusterTreeSetWritable message;

  // TODO SHOULD BE STATIC RIGHT?
  // Should be actually removed!!!
  /** Configuration */
  private ImmutableClassesGiraphConfiguration
    <LongWritable, ?, ?, SemiClusterTreeSetWritable> conf;

  /** Constructor */
  public MessageWrapper() {
  }

  /**
   * Constructor.
   * @param sourceId Vertex Source Id
   * @param message Message
   */
  public MessageWrapper(LongWritable sourceId,
      SemiClusterTreeSetWritable message) {
    this.sourceId = sourceId;
    this.message = message;
  }

  /**
   * Return Vertex Source Id
   *
   * @return sourceId Message sender vertex Id
   */
  public LongWritable getSourceId() {
    return sourceId;
  }

  public void setSourceId(LongWritable sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * Return Message data
   *
   * @return message message to be returned
   */
  public SemiClusterTreeSetWritable getMessage() {
    return message;
  }

  /**
   * Store message to this object
   *
   * @param message Message to be stored
   */
  public void setMessage(SemiClusterTreeSetWritable message) {
    this.message = message;
  }

  /**
   * Get Configuration
   *
   * @return conf Configuration
   */
  public ImmutableClassesGiraphConfiguration
      <LongWritable, ?, ?, SemiClusterTreeSetWritable> getConf() {
    return conf;
  }

  /**
   * Set Configuration
   *
   * @param conf Configuration to be stored
   */
  public void setConf(ImmutableClassesGiraphConfiguration
      <LongWritable, ?, ?, SemiClusterTreeSetWritable> conf) {
    this.conf = conf;
  }

  /**
   * Read Fields
   *
   * @param input Input
   */
  @Override
  public void readFields(DataInput input) throws IOException {
    sourceId = new LongWritable();
    toString();
    sourceId.readFields(input);
    message = new SemiClusterTreeSetWritable();
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
    MessageWrapper that = (MessageWrapper) o;

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
  public int compareTo(MessageWrapper wrapper) {

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
