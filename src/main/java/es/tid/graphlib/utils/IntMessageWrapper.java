package es.tid.graphlib.utils;

import org.apache.hadoop.io.IntWritable;

/** This class provides the wrapper for the sending message.*/
public class IntMessageWrapper
extends MessageWrapper<IntWritable, DoubleArrayListWritable> {

  /**
   * Default constructor for reflection.
   */
  public IntMessageWrapper() {
    super();
  }

  /**
   * Constructor with SourceId and Message to be used internally.
   *
   * @param pSourceId Vertex Source Id
   * @param pMessage Message to be sent
   */
  public IntMessageWrapper(final IntWritable pSourceId,
    final DoubleArrayListWritable pMessage) {
    super(pSourceId, pMessage);
  }

  /**
   * Provide class for the Vertex Source Id.
   *
   * @return class
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public final Class getVertexIdClass() {
    return IntWritable.class;
  }

  /**
   * Provide class for the Message to be sent.
   *
   * @return class
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public final Class getMessageClass() {
    return DoubleArrayListWritable.class;
  }
}
