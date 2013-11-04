package ml.grafos.okapi.io.formats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Writes the graph in an adjacency list format without any value or edge
 * values. This simply extends 
 * {@link AdjacencyListNoValuesTextVertexOutputFormat} with specific types.
 * 
 * @author dl
 *
 */
public class SimpleAdjacencyList extends
    AdjacencyListNoValuesTextVertexOutputFormat<LongWritable, DoubleWritable, 
    DoubleWritable> {

}
