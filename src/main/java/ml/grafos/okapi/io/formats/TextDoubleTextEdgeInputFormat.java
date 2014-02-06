/**
 * Copyright 2014 Grafos.ml
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with Text ids and Double as weights
 *
 * Each line consists of: source_vertex, target_vertex, edge value
 */
public class TextDoubleTextEdgeInputFormat extends
    TextEdgeInputFormat<Text, DoubleWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<Text, DoubleWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new TextDoubleTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link TextDoubleTextEdgeInputFormat}.
   */
  public class TextDoubleTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String> {
    @Override
    protected String preprocessLine(Text line) throws IOException {
      return line.toString();
    }

    @Override
    protected Text getSourceVertexId(String line) throws IOException {
      String[] tokens = SEPARATOR.split(line);
      return new Text(tokens[0]);
    }

    @Override
    protected Text getTargetVertexId(String line) throws IOException {
      String[] tokens = SEPARATOR.split(line);
      return new Text(tokens[1]);
    }

    @Override
    protected DoubleWritable getValue(String line) throws IOException {
      String[] tokens = SEPARATOR.split(line);
      return new DoubleWritable(Double.parseDouble(tokens[2]));
    }
  }
}
