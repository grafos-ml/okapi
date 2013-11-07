package ml.grafos.okapi.cf.ranking;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads the data in edge format:
 * 
 * userId \t itemId \t score
 * ...
 * userId \t itemId \t score
 * 
 * @author linas
 *
 */
public class LongLongIntInputFormat extends TextEdgeInputFormat<LongWritable, IntWritable>{

	@Override
	public EdgeReader<LongWritable, IntWritable> createEdgeReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		// TODO Auto-generated method stub
		return new LongLongIntEdge();
	}
	
	class LongLongIntEdge extends TextEdgeReaderFromEachLine{
		@Override
		protected LongWritable getSourceVertexId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split(" ")[0]));
		}

		@Override
		protected LongWritable getTargetVertexId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split(" ")[1]));
		}

		@Override
		protected IntWritable getValue(Text line) throws IOException {
			return new IntWritable(Integer.parseInt(line.toString().split(" ")[2]));
		}
	}
}
