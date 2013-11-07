package ml.grafos.okapi.cf.eval;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A custom reader that reads input formated as (First line is needed for the final output).
 * 0	\t	0.0	
 * user1 \t feature1,feature2,feature3 \t itemid1,item2,item3
 * ...
 * userM \t feature1,feature2,feature3 \t itemid1,item2,item3
 * item1 \t feature1,feature2,feature3 \t 
 * ...
 * itemN \t feature1,feature2,feature3 \t 
 * 
 * user ids should start from 1, item ids should be negatives, 0 is a reserved id for the final output.
 * 
 * @author linas
 *
 */
public class LongArrayBooleanInputFormat extends TextVertexInputFormat<LongWritable, DoubleArrayListWritable, BooleanWritable>{

	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongArrayBooleanVertexReader();
	}
	
	class LongArrayBooleanVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			return line.toString().split("\t");
		}

		@Override
		protected LongWritable getId(String[] line) throws IOException {
			return new LongWritable(Long.parseLong(line[0]));
		}

		@Override
		protected DoubleArrayListWritable getValue(String[] line)
				throws IOException {
			String[] factors = line[1].split(",");
			DoubleArrayListWritable daw = new DoubleArrayListWritable();
			for (String f : factors) {
				daw.add(new DoubleWritable(Double.parseDouble(f)));
			}
			return daw;
		}

		@Override
		protected Iterable<Edge<LongWritable, BooleanWritable>> getEdges(
				String[] line) throws IOException {
			List<Edge<LongWritable, BooleanWritable>> ret = new LinkedList<Edge<LongWritable,BooleanWritable>>();
			if (line.length == 3){
				String[] ids = line[2].split(",");
				for (String id : ids) {
					ret.add(EdgeFactory.create(new LongWritable(Long.parseLong(id)), new BooleanWritable(true)));
				}
			}
			return ret;
		}
	}
}