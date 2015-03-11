package ml.grafos.okapi.iFub;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LongLongEdgesListInputFormat extends
			TextEdgeInputFormat<LongWritable, NullWritable> {
		/** Splitter for endpoints */
		private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

		@Override
		public EdgeReader<LongWritable, NullWritable> createEdgeReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException {
			return new LongLongEdgesReader();
		}

		public class LongLongEdgesReader extends
				TextEdgeReaderFromEachLineProcessed<String[]> {
			@Override
			protected String[] preprocessLine(Text line) throws IOException {
				return SEPARATOR.split(line.toString());
			}

			@Override
			protected LongWritable getSourceVertexId(String[] endpoints)
					throws IOException {
				return new LongWritable(Long.parseLong(endpoints[0]));
			}

			@Override
			protected LongWritable getTargetVertexId(String[] endpoints)
					throws IOException {
				return new LongWritable(Long.parseLong(endpoints[1]));
			}

			@Override
			protected NullWritable getValue(String[] endpoints) throws IOException {

				return NullWritable.get();
			}
		}
	}