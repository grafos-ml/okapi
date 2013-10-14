package es.tid.graphlib.examples;

import org.apache.giraph.examples.IdentityComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * This is simply an extension of the 
 * {@link org.apache.giraph.examples.IdentityComputation IdentityComputation}
 * with specific parameter types.
 * 
 * @author dl
 *
 */
public class SimpleIdentityComputation extends IdentityComputation<LongWritable,
  DoubleWritable, DoubleWritable, DoubleWritable> {

}
