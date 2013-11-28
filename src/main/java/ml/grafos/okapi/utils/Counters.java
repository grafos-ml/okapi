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
