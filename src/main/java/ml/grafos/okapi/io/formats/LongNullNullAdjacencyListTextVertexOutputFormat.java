package ml.grafos.okapi.io.formats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

public class LongNullNullAdjacencyListTextVertexOutputFormat extends
    AdjacencyListNoValuesTextVertexOutputFormat<
      LongWritable, NullWritable, NullWritable> {

}
