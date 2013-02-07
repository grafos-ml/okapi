package es.tid.graphlib.triangles;

import org.apache.giraph.examples.SimpleTriangleClosingVertex;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntIntNullIntTextInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AppRunner implements Tool {

  private Configuration conf;
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "run: Must have 3 arguments <input path> <output path> " +
          "<# of workers>");
    }
    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.getConfiguration().setVertexClass(SimpleTriangleClosingVertex.class);
    job.getConfiguration().setVertexInputFormatClass(IntIntNullIntTextInputFormat.class);
    job.getConfiguration().setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    GiraphFileInputFormat.addVertexInputPath(job.getInternalJob(), new Path(args[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(args[1]));
    job.getConfiguration().setWorkerConfiguration(Integer.parseInt(args[2]),
        Integer.parseInt(args[2]),
        100.0f);
    if (job.run(true) == true) {
      return 0;
    } else {
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AppRunner(), args));
  }
}
