package ml.grafos.okapi.io.formats;

import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class LongDoubleDoubleAdjacencyListTextVertexOutputFormat extends
    AdjacencyListTextVertexOutputFormat<LongWritable, DoubleWritable, 
    DoubleWritable> {

}
