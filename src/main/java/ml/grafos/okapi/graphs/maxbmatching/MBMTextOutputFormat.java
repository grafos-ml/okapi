package ml.grafos.okapi.graphs.maxbmatching;

import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class MBMTextOutputFormat extends AdjacencyListTextVertexOutputFormat<LongWritable, IntWritable, MBMEdgeState> {

}
