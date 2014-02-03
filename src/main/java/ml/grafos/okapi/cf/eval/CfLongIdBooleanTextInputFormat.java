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
package ml.grafos.okapi.cf.eval;

import java.io.IOException;
import java.util.regex.Pattern;

import ml.grafos.okapi.cf.CfLongId;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} to read
 * user-item ratings as the input for the CF algorithms.
 *
 * Each line consists of: <user id> <item id> <rating (float)>
 */
public class CfLongIdBooleanTextInputFormat extends
    TextEdgeInputFormat<CfLongId, BooleanWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<CfLongId, BooleanWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new CfIdFloatTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class CfIdFloatTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected CfLongId getSourceVertexId(String[] tokens)
      throws IOException {
      // type 0 is user
      return new CfLongId((byte)0, Integer.parseInt(tokens[0]));
    }

    @Override
    protected CfLongId getTargetVertexId(String[] tokens)
      throws IOException {
      // type 1 is item
      return new CfLongId((byte)1, Integer.parseInt(tokens[1]));
    }

    @Override
    protected BooleanWritable getValue(String[] tokens) throws IOException {
    		return new BooleanWritable(true);
    }
  }
}
