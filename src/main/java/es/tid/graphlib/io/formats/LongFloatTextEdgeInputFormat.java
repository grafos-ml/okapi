/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.tid.graphlib.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.tid.graphlib.utils.LongPairVal;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with int ids int values.
 *
 * Each line consists of: source_vertex, target_vertex
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
      TextEdgeReaderFromEachLineProcessed<LongPairVal> {
    @Override
    protected LongPairVal preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      return new LongPairVal(Long.valueOf(tokens[0]),
          Long.valueOf(tokens[1]), Float.valueOf(tokens[2]));
    }

    @Override
    protected LongWritable getSourceVertexId(LongPairVal endpoints)
      throws IOException {
      return new LongWritable(endpoints.getFirst());
    }

    @Override
    protected LongWritable getTargetVertexId(LongPairVal endpoints)
      throws IOException {
      return new LongWritable(endpoints.getSecond());
    }

    @Override
    protected FloatWritable getValue(LongPairVal endpoints)
      throws IOException {
      return new FloatWritable((float) endpoints.getValue());
    }
  }
}
