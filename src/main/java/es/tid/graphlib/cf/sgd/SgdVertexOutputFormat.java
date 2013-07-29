/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package es.tid.graphlib.cf.sgd;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for graphs
 * with int ids.
 *
 * Each line consists of: vertex id, vertex value and option edge value
 */
public class SgdVertexOutputFormat
  extends TextVertexOutputFormat
  <IntWritable, SgdVertexValueType, DoubleWritable> {

  /** Specify the output delimiter */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /** Default output delimiter */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "   ";
  /**
   * Create Vertex Writer
   *
   * @param context Context
   * @return new object TextIntIntVertexWriter
   */
  public TextVertexWriter
  createVertexWriter(TaskAttemptContext context) {
    return new SgdVertexWriter();
  }
  /** Class TextIntIntVertexWriter */
  protected class SgdVertexWriter
      extends TextVertexWriterToEachLine {
    /** Saved delimiter */
    private String delimiter;

    @Override
    public void initialize(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(context);
      Configuration conf = context.getConfiguration();
      delimiter = conf
        .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    protected Text convertVertexToLine(Vertex
      <IntWritable, SgdVertexValueType, DoubleWritable, ?> vertex)
      throws IOException {
      boolean isErrorFlag = getContext().getConfiguration().getBoolean(
        "sgd.print.error", false);
      boolean isUpdatesFlag = getContext().getConfiguration().getBoolean(
        "sgd.print.updates", false);
      boolean isMessagesFlag = getContext().getConfiguration().getBoolean(
        "sgd.print.messages", false);

      String type = "";
      if (((Sgd) vertex).isItem()) {
        type = "item";
      } else {
        type = "user";
      }
      String id = vertex.getId().toString();
      String value = vertex.getValue().getLatentVector().toString();
      String error = null;
      String updates = null;
      String messages = null;
      Text line = new Text(type + delimiter + id + delimiter + value);

      if (isErrorFlag) {
        error = Double.toString(Math.abs(((Sgd) vertex).getHaltFactor()));
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(error.getBytes(), 0, error.length());
      }
      if (isUpdatesFlag) {
        updates = Integer.toString(((Sgd) vertex).getUpdates());
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(updates.getBytes(), 0, updates.length());
      }
      if (isMessagesFlag) {
        messages = Integer.toString(((Sgd) vertex).getMessages());
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(messages.getBytes(), 0, messages.length());
      }
      return new Text(line);
    }
  }
}
