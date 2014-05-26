package ml.grafos.okapi.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Contains helper methods for manipulating Writable objects.
 * 
 * @author dl
 *
 */
public class WritableUtility {

  /**
   * Clones a writable object using the Hadoop ReflectionUtils object.
   * @param object The object to clone
   * @param conf The configuration
   * @return The cloned object
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws IOException
   */
  public static Writable clone(Writable object, Configuration conf) 
      throws InstantiationException, IllegalAccessException, IOException {
    Writable cloned = null;
    cloned = object.getClass().newInstance();
    ReflectionUtils.copy(conf, object, cloned);
    return cloned;
  }
}
