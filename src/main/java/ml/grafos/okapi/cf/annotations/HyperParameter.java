package ml.grafos.okapi.cf.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface HyperParameter {
	String parameterName(); //the name of the parameter that is used to pass to the giraph with ca option, for example delta
	String description(); //description of the parameter
	float defaultValue(); //default parameter value
	float minimumValue(); //minimum value parameter can take (for automatic search)
	float maximumValue(); //maximum value parameter can take (for automatic search)
}
