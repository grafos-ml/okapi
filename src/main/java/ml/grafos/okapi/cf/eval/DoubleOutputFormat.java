package ml.grafos.okapi.cf.eval;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class DoubleOutputFormat extends TextVertexOutputFormat<LongWritable, DoubleArrayListWritable, BooleanWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DoubleVertexWriter();
	}

	class DoubleVertexWriter extends TextVertexWriterToEachLine{

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex)
				throws IOException {
			if (vertex.getId().get() == 0){
				return new Text(vertex.getValue().get(0).toString());
			}
			return null;
		}
	}
}
