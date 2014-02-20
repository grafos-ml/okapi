package ml.grafos.okapi.aggregators;

import org.apache.hadoop.io.Writable;

/**
 * An aggregator writer that only writes the values of aggregators.
 * @author dl
 *
 */
public class ValueTextAggregatorWriter extends TextAggregatorWriter {
  @Override
  protected String aggregatorToString(String aggregatorName, Writable value, 
      long superstep) {
    return value+"\n";
  }
}