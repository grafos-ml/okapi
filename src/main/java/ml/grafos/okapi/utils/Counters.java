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
package ml.grafos.okapi.utils;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Utility class that helps maintain Hadoop counters.
 * 
 * @author dl
 *
 */
public class Counters {

  /**
   * Replaces the value of a counter with a new one. 
   * 
   * @param context
   * @param counterGroup
   * @param counterName
   * @param newValue
   */
  public static void updateCounter(Context context, String counterGroup, 
      String counterName, long newValue) {
    
    Counter counter = context.getCounter(counterGroup, counterName);
    long oldValue = counter.getValue();
    counter.increment(newValue-oldValue);
  }

  /**
   * Increments the value of a counter.
   * 
   * @param context
   * @param counterGroup
   * @param counterName
   * @param increment
   */
  public static void incrementCounter(Context context, String counterGroup, 
      String counterName, long increment) {
    
    context.getCounter(counterGroup, counterName).increment(increment);
  }
}
