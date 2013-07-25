package es.tid.graphlib.utils;

import org.apache.hadoop.io.Text;


/** This class provides the wrapper for the sending message.*/
public class TextMessageWrapper
extends MessageWrapper<Text, DoubleArrayListWritable> {

  /**
   * Default constructor for reflection.
   */
  public TextMessageWrapper() {
    super();
  }

  /**
   * Constructor with SourceId and Message to be used internally.
   *
   * @param pSourceId Vertex Source Id
   * @param pMessage Message to be sent
   */
  public TextMessageWrapper(final Text pSourceId,
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
    return Text.class;
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
