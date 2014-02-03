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
package ml.grafos.okapi.examples;

import org.apache.giraph.examples.IdentityComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * This is simply an extension of the 
 * {@link org.apache.giraph.examples.IdentityComputation IdentityComputation}
 * with specific parameter types.
 * 
 * @author dl
 *
 */
public class SimpleIdentityComputation extends IdentityComputation<LongWritable,
  DoubleWritable, DoubleWritable, DoubleWritable> {

}
