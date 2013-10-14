package es.tid.graphlib.io.formats;

import org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Used to output the edges of a graph along with their values. It assumes an
 * id of type long, a vertex value of type double (not used) and an edge value
 * of type double. 
 * 
 * @author dl
 *
 */
public class LongDoubleTextEdgeOutputFormat extends
    SrcIdDstIdEdgeValueTextOutputFormat<LongWritable, DoubleWritable, 
    DoubleWritable> {

}
