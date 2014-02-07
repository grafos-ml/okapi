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
package ml.grafos.okapi.common;

import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.LongConfOption;

/**
 * Common Okapi parameters.  
 */
public class Parameters {

  public static LongConfOption RANDOM_SEED = 
      new LongConfOption("random.seed", -1, 
          "Random number generator seed");

  public static BooleanConfOption GRAPH_DIRECTED =
      new BooleanConfOption("graph.directed", true, 
          "Defines whether the graph is directed or not. "
          + "By default a graph is considered directed.");
}
