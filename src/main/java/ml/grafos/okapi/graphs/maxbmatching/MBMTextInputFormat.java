package ml.grafos.okapi.graphs.maxbmatching;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The input format for the maximum b-matching algorithm. The input is an undirected graph in adjacency list format, one vertex per line. The algorithm assumes
 * that each edge is present twice in the input, once for each end-point. The line is composed by a vertex, and a list of edges. The vertex is composed by a long
 * id, and an integer capacity. Each edge is composed by a long id (the destination vertex id), and a double weight. The elements of the list are tab or space
 * separated.
 * 
 * E.g., "1 5 2 0.5 3 0.1" creates a vertex with id=1, capacity=5, and two edges to vertices 2 and 3, with weights 0.5 and 0.1, respectively.
 */
public class MBMTextInputFormat extends AdjacencyListTextVertexInputFormat<LongWritable, IntWritable, MBMEdgeValue> {

    @Override
    public AdjacencyListTextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new MBMVertexReader();
    }

    public class MBMVertexReader extends AdjacencyListTextVertexReader {

        @Override
        public Edge<LongWritable, MBMEdgeValue> decodeEdge(String e, String w) {
            return EdgeFactory.create(new LongWritable(Long.parseLong(e)), new MBMEdgeValue(Double.parseDouble(w)));
        }

        @Override
        public LongWritable decodeId(String s) {
            return new LongWritable(Long.parseLong(s));
        }

        @Override
        public IntWritable decodeValue(String s) {
            return new IntWritable(Integer.parseInt(s));
        }
    }
}
