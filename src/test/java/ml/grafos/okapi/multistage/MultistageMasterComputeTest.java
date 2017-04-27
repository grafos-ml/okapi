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
package ml.grafos.okapi.multistage;

import ml.grafos.okapi.multistage.voting.TransitionElection;
import ml.grafos.okapi.multistage.voting.UnanimityElection;
import org.apache.giraph.aggregators.IntMaxAggregator;
import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Test class for the MultistageMasterCompute class.
 *
 * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es
 */
public class MultistageMasterComputeTest {
  private static Logger logger = Logger.getLogger(MultistageMasterComputeTest.class);

  @Test
  public void testMultistageAlgorithm() {
    String[] graph = {
        "1 3",
        "2 2",
        "3 -2",
        "4 8",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(MinComputationStage.class);
    conf.setMasterComputeClass(AlgorithmMaster.class);
    conf.setVertexInputFormatClass(TestInputFormatter.class);
    conf.setVertexOutputFormatClass(TestOutputFormat.class);
    Iterable<String> results;
    try {
      results = InternalVertexRunner.run(conf, graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int size = 0;
    for (String string : results) {
      size++;
      Assert.assertEquals(string, "-2,8");
      logger.debug(string);
    }
    Assert.assertEquals(4, size);
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Multistage algorithm implementation

  /**
   * Enumerates the stages of the algorithm, and which computation should run
   * on each of them. The computation classes are defined below.
   */
  public static enum AlgorithmStages implements Stage {
    MIN(MinComputationStage.class),
    MAX(MaxComputationStage.class),
    FINAL(FinalComputationStage.class),
    ;

    private final Class<? extends StageComputation> clazz;

    AlgorithmStages(Class<? extends StageComputation> clazz) {
      this.clazz = clazz;
    }

    @Override
    public Class<? extends StageComputation> getStageClass() {
      return clazz;
    }
  }

  /**
   * Master of this multi-stage algorithm.
   */
  public static class AlgorithmMaster extends MultistageMasterCompute {

    /**
     * Custom initialization code. Do not forger to call {@code super.initialize()}!
     *
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
      super.initialize();

      registerPersistentAggregator("min", IntMinAggregator.class);
      registerPersistentAggregator("max", IntMaxAggregator.class);
    }

    @Override
    public TransitionElection getTransitionElection() {
      return new UnanimityElection();
    }

    @Override
    public Stage[] getStages() {
      return AlgorithmStages.values();
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Computations to perform at each stage

  public static class MinComputationStage extends StageComputation
      <LongWritable, IntMinMaxValue, NullWritable, NullWritable, NullWritable> {
    @Override
    public void compute(Vertex<LongWritable, IntMinMaxValue, NullWritable> vertex, Iterable<NullWritable> messages) throws IOException {
      aggregate("min", new IntWritable(vertex.getValue().value));
      voteToTransition(AlgorithmStages.MAX);
    }
  }

  public static class MaxComputationStage extends StageComputation
      <LongWritable, IntMinMaxValue, NullWritable, NullWritable, NullWritable> {
    @Override
    public void compute(Vertex<LongWritable, IntMinMaxValue, NullWritable> vertex, Iterable<NullWritable> messages) throws IOException {
      IntWritable min = getAggregatedValue("min");
      vertex.getValue().min = min.get();
      aggregate("max", new IntWritable(vertex.getValue().value));
      voteToTransition(AlgorithmStages.FINAL);
    }
  }

  public static class FinalComputationStage extends StageComputation
      <LongWritable, IntMinMaxValue, NullWritable, NullWritable, NullWritable> {
    @Override
    public void compute(Vertex<LongWritable, IntMinMaxValue, NullWritable> vertex, Iterable<NullWritable> messages) throws IOException {
      IntWritable max = getAggregatedValue("max");
      vertex.getValue().max = max.get();
      vertex.voteToHalt();
    }
  }


  //////////////////////////////////////////////////////////////////////////////////
  // Regular computation stuff

  public static class TestInputFormatter
      extends TextVertexValueInputFormat<LongWritable, IntMinMaxValue, NullWritable> {

    private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

    @Override
    public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
      return new APInputReader();
    }

    public class APInputReader extends TextVertexValueReaderFromEachLineProcessed<String[]> {

      @Override
      protected String[] preprocessLine(Text line) throws IOException {
        return SEPARATOR.split(line.toString());
      }

      @Override
      protected LongWritable getId(String[] line) throws IOException {
        return new LongWritable(Long.valueOf(line[0]));
      }

      @Override
      protected IntMinMaxValue getValue(String[] line) throws IOException {
        return new IntMinMaxValue(Integer.valueOf(line[1]));
      }
    }

  }

  public static class TestOutputFormat
      extends TextVertexOutputFormat<LongWritable, IntMinMaxValue, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
      return new IdWithValueVertexWriter();
    }

    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
      @Override
      protected Text convertVertexToLine(Vertex<LongWritable, IntMinMaxValue,
          NullWritable> vertex) throws IOException {

        StringBuilder str = new StringBuilder().append(vertex.getValue().min)
            .append(",").append(vertex.getValue().max);
        return new Text(str.toString());
      }
    }
  }

  public static class IntMinMaxValue implements Writable {
    public int value;
    public int min;
    public int max;

    public IntMinMaxValue() {
      this(0);
    }

    public IntMinMaxValue(int value) {
      this.value = value;
      this.min = Integer.MAX_VALUE;
      this.max = Integer.MIN_VALUE;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(value);
      dataOutput.writeInt(min);
      dataOutput.writeInt(max);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      value = dataInput.readInt();
      min = dataInput.readInt();
      max = dataInput.readInt();
    }
  }

}
