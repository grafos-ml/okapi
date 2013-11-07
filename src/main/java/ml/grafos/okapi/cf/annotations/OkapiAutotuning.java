package ml.grafos.okapi.cf.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Used to indicate that the method could be autotuned by okapi tuning script.
 * @author linas
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface OkapiAutotuning {

}
