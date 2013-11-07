package ml.grafos.okapi.cf.ranking;

import java.io.IOException;

import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class TrainingForEval extends TextVertexOutputFormat<LongWritable, DoubleArrayListWritable, IntWritable>{

	@Override
	public org.apache.giraph.io.formats.TextVertexOutputFormat.TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new DoubleVertexWriter();
	}
	
	class DoubleVertexWriter extends TextVertexWriterToEachLine{

		@Override
		protected Text convertVertexToLine(
				Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex)
				throws IOException {
				StringBuilder sb = new StringBuilder();
				//add ID
				sb.append(vertex.getId().toString()).append("\t");
				
				//add factors
				DoubleArrayListWritable value = vertex.getValue();
				StringBuilder arr = new StringBuilder();
				for (DoubleWritable doubleWritable : value) {
					if (arr.length() > 0){
						arr.append(",");
					}
					arr.append(doubleWritable.toString());
				}
				//for users add the items
				StringBuilder outEdges = new StringBuilder();
				if (vertex.getId().get() > 0){
					Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
					for (Edge<LongWritable, IntWritable> edge : edges) {
						if (outEdges.length() > 0){
							outEdges.append(",");
						}
						outEdges.append(edge.getTargetVertexId());
					}
				}
				sb.append(arr);
				sb.append("\t").append(outEdges);
			
				return new Text(sb.toString());
		}
	}

}
