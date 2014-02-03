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

import org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Used to output the edges of a graph along with their values. It assumes an
 * id of type long, a vertex value of type double (not used) and an edge value
 * of type double. 
 * 
 * @author dl
 *
 */
public class LongDoubleTextEdgeOutputFormat extends
    SrcIdDstIdEdgeValueTextOutputFormat<LongWritable, DoubleWritable, 
    DoubleWritable> {

}
