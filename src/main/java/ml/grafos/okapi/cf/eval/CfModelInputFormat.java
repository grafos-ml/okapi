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
package ml.grafos.okapi.cf.eval;

import java.io.IOException;
import java.util.ArrayList;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jblas.FloatMatrix;

/**
 * A custom reader that reads the model formated as following example.
 *
 * nodeId	computed_model
 * 32729 0	[0.883140; 0.126675]
 * 7563 0	[0.544951; 0.719476]
 * 5007 1	[0.726413; 0.968422]
 * 1384 1	[0.933587; 0.755566]
 * 304 1	[0.368630; 0.468095]
 *
 * This can be obtained by running runOkapi.py.
 * 
 * @author linas
 *
 */
public class CfModelInputFormat extends TextVertexValueInputFormat<CfLongId, FloatMatrixWritable, FloatWritable>{

  public static String DIMENSION = "dim";

	@Override
	public TextVertexValueReader createVertexValueReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongArrayBooleanVertexValueReader();
	}
	
	public class LongArrayBooleanVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<String[]> {

		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			return line.toString().split("\t");
		}

		@Override
		protected CfLongId getId(String[] line) throws IOException {
			 String id = line[0];
            String[] id_type = id.split(" ");
			return new CfLongId((byte)Integer.parseInt(id_type[1]), Long.parseLong(id_type[0]));
		}

		@Override
		protected FloatMatrixWritable getValue(String[] line)
				throws IOException {
			if (line.length > 1){//for users and items
				String[] factors = line[1].split("\\[|\\]|,|;");
				ArrayList<Float> factorsFloat = new ArrayList<Float>();
				for(int i=0; i<factors.length; i++){
					if (!factors[i].trim().isEmpty())
						factorsFloat.add(Float.parseFloat(factors[i]));
				}
				FloatMatrix array = new FloatMatrix(factorsFloat);
				
				// If you specify the dimension of the latent vector, we will
				// do a check to ensure the computed model has the right dimension
				if ((getConf().getInt(DIMENSION, -1))!=-1) {
				  if (array.length!=getConf().getInt(DIMENSION, -1)) {
				    throw new RuntimeException("Latent vector size is incorrect");
				  }
				}

				return new FloatMatrixWritable(array);
			}else{//for null node
				return new FloatMatrixWritable(0);
			}
		}
	}
}