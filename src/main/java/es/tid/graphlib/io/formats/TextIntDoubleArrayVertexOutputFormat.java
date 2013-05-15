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
package es.tid.graphlib.io.formats;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.tid.graphlib.sgd.SgdGeneral;
import es.tid.graphlib.utils.DoubleArrayListWritable;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for graphs
 * with int ids.
 *
 * Each line consists of: vertex id, vertex value and option edge value
 */
public class TextIntDoubleArrayVertexOutputFormat extends
  TextVertexOutputFormat<IntWritable, DoubleArrayListWritable,
  IntWritable> {

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
    return new TextIntDoubleArrayVertexWriter();
  }

  /** Class TextIntIntVertexWriter */
  protected class TextIntDoubleArrayVertexWriter
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
    protected Text convertVertexToLine
    (Vertex<IntWritable, DoubleArrayListWritable, IntWritable, ?> vertex)
      throws IOException {

      boolean flag = getContext().getConfiguration().getBoolean(
        "sgd.printerr", false);
      String id = vertex.getId().toString();
      String value = vertex.getValue().toString();
      String error = null;
      Text line;
      if (flag) {
        error = Double
            .toString(Math.abs(((SgdGeneral) vertex).err_factor));
        line = new Text(id + delimiter + value + delimiter + error);
      } else {
        line = new Text(id + delimiter + value);
      }
      return new Text(line);
    }
  }
}
