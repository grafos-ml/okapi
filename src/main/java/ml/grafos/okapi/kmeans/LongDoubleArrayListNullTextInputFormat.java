package ml.grafos.okapi.kmeans;

import java.io.IOException;
import java.util.regex.Pattern;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LongDoubleArrayListNullTextInputFormat extends
		TextVertexInputFormat<LongWritable, DoubleArrayListWritable, NullWritable> {
	  /** Separator of the vertex id and its coordinates */
	  private static final Pattern ID_SEPARATOR = Pattern.compile("[,]");
	  /** Separator of the coordinates values */
	  private static final Pattern COORD_SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongDoubleArrayListNullVertexReader();
	}
	
	public class LongDoubleArrayListNullVertexReader extends 
		TextVertexReaderFromEachLineProcessed<String[]> {
		/** Vertex id for the current line */
	    private LongWritable id;
	    /** The vertex coordinates*/
	    private String coordinatesString;

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] tokens = ID_SEPARATOR.split(line.toString());
		    id = new LongWritable(Long.parseLong(tokens[0]));
		    coordinatesString = tokens[1];
		    return tokens;
		}

		@Override
		protected LongWritable getId(String[] line) throws IOException {
			return id;
		}

		@Override
		protected DoubleArrayListWritable getValue(String[] line)
				throws IOException {
			DoubleArrayListWritable coordinates = new DoubleArrayListWritable();
			String[] tokens = COORD_SEPARATOR.split(coordinatesString);
			for ( int i = 0; i < tokens.length; i++ ) {
				coordinates.add(new DoubleWritable(Double.parseDouble(tokens[i])));
			}
			return coordinates;
		}

		@Override
		protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
				String[] line) throws IOException {
			// no edges
			return null;
		}
		
	}

}
