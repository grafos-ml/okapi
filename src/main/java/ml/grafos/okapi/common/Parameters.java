package ml.grafos.okapi.common;

import org.apache.giraph.conf.BooleanConfOption;

/**
 * Common Okapi parameters.  
 */
public class Parameters {

  public static BooleanConfOption GRAPH_DIRECTED =
      new BooleanConfOption("graph.directed", true, 
          "Defines whether the graph is directed or not. "
          + "By default a graph is considered directed.");
}
