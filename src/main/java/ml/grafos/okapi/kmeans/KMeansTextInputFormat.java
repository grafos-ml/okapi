package ml.grafos.okapi.kmeans;

import java.io.IOException;
import java.util.regex.Pattern;
import ml.grafos.okapi.common.data.DoubleArrayListWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** 
 * The input format for the k-means algorithm.
 * It reads form a file, where each line contains one point.
 * The line is formed by an integer id and the double coordinates of the input point.
 * The id is separated from the coordinates by comma, while the coordinates are tab-separated.
 * The produced vertex value contains the point coordinates and an empty initial cluster center.
 */
public class KMeansTextInputFormat extends
		TextVertexInputFormat<LongWritable, KMeansVertexValue, NullWritable> {
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
		protected KMeansVertexValue getValue(String[] line)
				throws IOException {
			DoubleArrayListWritable coordinates = new DoubleArrayListWritable();
			String[] tokens = COORD_SEPARATOR.split(coordinatesString);
			for ( int i = 0; i < tokens.length; i++ ) {
				coordinates.add(new DoubleWritable(Double.parseDouble(tokens[i])));
			}
			IntWritable clusterId = new IntWritable();
			return new KMeansVertexValue(coordinates, clusterId);
		}

		@Override
		protected Iterable<Edge<LongWritable, NullWritable>> getEdges(
				String[] line) throws IOException {
			// no edges
			return null;
		}
		
	}

}
