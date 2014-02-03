/**
 * Copyright 2013 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with int ids int values. In this format however,
 * the value of the edge is not expected in the input.
 *
 * Each line consists of: <src id> <dst id>
 */
public class LongDoubleDefaultEdgeValueTextEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, DoubleWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongDoubleTextEdgeReader(context);
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class LongDoubleTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    
    DoubleWritable defaultEdgeValue; 
        
    public LongDoubleTextEdgeReader(TaskAttemptContext context) {
      defaultEdgeValue = new DoubleWritable(
          Double.parseDouble(
              context.getConfiguration().get("edge.default.value")));
    }
        
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected LongWritable getSourceVertexId(String[] tokens)
      throws IOException {
      return new LongWritable(Long.parseLong(tokens[0]));
    }

    @Override
    protected LongWritable getTargetVertexId(String[] tokens)
      throws IOException {
      return new LongWritable(Long.parseLong(tokens[1]));
    }

    @Override
    protected DoubleWritable getValue(String tokens[]) throws IOException {
      return defaultEdgeValue;
    }
  }
}
