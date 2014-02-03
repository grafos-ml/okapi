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
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with long ids float values.
 *
 * Each line consists of: <src id> <target id> <edge weight>
 */
public class LongFloatTextEdgeInputFormat extends
    TextEdgeInputFormat<LongWritable, FloatWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<LongWritable, FloatWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new LongLongFloatTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link LongLongDoubleTextEdgeInputFormat}.
   */
  public class LongLongFloatTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
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
    protected FloatWritable getValue(String[] tokens)
      throws IOException {
      return new FloatWritable(Float.parseFloat(tokens[2]));
    }
  }
}
