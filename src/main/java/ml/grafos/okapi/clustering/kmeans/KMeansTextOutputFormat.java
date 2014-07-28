package ml.grafos.okapi.clustering.kmeans;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class KMeansTextOutputFormat extends 
		TextVertexOutputFormat<LongWritable, KMeansVertexValue, NullWritable> {

	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new LongIntVertexNullVertexWriter();
	}

	protected class LongIntVertexNullVertexWriter extends 
			TextVertexWriterToEachLine {
		private String delimiter = ",";

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, KMeansVertexValue, NullWritable> vertex)
				throws IOException {
			StringBuffer sb = new StringBuffer(vertex.getId().toString());
		    sb.append(delimiter);
		    sb.append(vertex.getValue().getClusterId().toString());
			return new Text(sb.toString());
		}
		
	}
}
