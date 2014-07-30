package ml.grafos.okapi.graphs.maxbmatching;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MBMTextOutputFormat extends AdjacencyListTextVertexOutputFormat<LongWritable, IntWritable, MBMEdgeValue> {

    @Override
    public AdjacencyListTextVertexOutputFormat<LongWritable, IntWritable, MBMEdgeValue>.AdjacencyListTextVertexWriter createVertexWriter(
            TaskAttemptContext context) {
        return new MBMVertexWriter();
    }

    public class MBMVertexWriter extends AdjacencyListTextVertexWriter {
        protected String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        public Text convertVertexToLine(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex) throws IOException {
            StringBuffer sb = new StringBuffer(vertex.getId().toString());
            sb.append(delimiter);
            sb.append(vertex.getValue());

            for (Edge<LongWritable, MBMEdgeValue> edge : vertex.getEdges()) {
                sb.append(delimiter).append(edge.getTargetVertexId());
                sb.append(delimiter).append(edge.getValue().getWeight());
                // skip the state, which is always INCLUDED
            }
            return new Text(sb.toString());
        }
    }
}
