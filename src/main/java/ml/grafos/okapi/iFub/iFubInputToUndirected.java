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
/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import cutGraph.LongLongEdgesListInputFormat;

import java.io.IOException;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with long ids.
 *
 * Each line consists of: source_vertex target_vertex
 *
 * This version also creates the reverse edges.
 */
public class iFubInputToUndirected
    extends LongLongEdgesListInputFormat {
  @Override
  public EdgeReader<LongWritable, NullWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    EdgeReader<LongWritable, NullWritable> edgeReader =
        super.createEdgeReader(split, context);
    edgeReader.setConf(getConf());
    return new ReverseEdgeDuplicator<LongWritable, NullWritable>(edgeReader);
  }
}
