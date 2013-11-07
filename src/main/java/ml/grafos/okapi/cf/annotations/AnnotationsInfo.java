package ml.grafos.okapi.cf.annotations;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Used to get JSON info for auto-tuning purposes. Uses reflection to find
 * methods and parameter ranges and spit it out into the stdout.
 * 
 * @author linas
 * 
 */
public class AnnotationsInfo {

	private String topPackage;
	
	public AnnotationsInfo(String pack) {
		this.topPackage = pack;
	}

	private Iterable<Class> getClasses(String packageName)
			throws ClassNotFoundException, IOException {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		String path = packageName.replace('.', '/');
		Enumeration<URL> resources = classLoader.getResources(path);
		List<File> dirs = new ArrayList<File>();
		while (resources.hasMoreElements()) {
			URL resource = resources.nextElement();
			dirs.add(new File(resource.getFile()));
		}
		List<Class> classes = new ArrayList<Class>();
		for (File directory : dirs) {
			classes.addAll(findClasses(directory, packageName));
		}

		return classes;
	}

	private List<Class> findClasses(File directory, String packageName)
			throws ClassNotFoundException {
		List<Class> classes = new ArrayList<Class>();
		if (!directory.exists()) {
			return classes;
		}
		File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				classes.addAll(findClasses(file,
						packageName + "." + file.getName()));
			} else if (file.getName().endsWith(".class")) {
				classes.add(Class.forName(packageName
						+ '.'
						+ file.getName().substring(0,
								file.getName().length() - 6)));
			}
		}
		return classes;
	}
	
	
	public JSONObject getInfo() throws ClassNotFoundException, IOException, JSONException {
		JSONObject obj = new JSONObject();
		ArrayList<JSONObject> cl = new ArrayList<JSONObject>();
		Iterable<Class> classes = getClasses(this.topPackage);
		for (Class c : classes) {
			JSONObject forClass = new JSONObject();
			if (c.getAnnotations().length > 0){
				
				ArrayList<JSONObject> parameters = new ArrayList<JSONObject>();
				for(Field field : c.getDeclaredFields()){
					if (field.isAnnotationPresent(HyperParameter.class)){
							HyperParameter hp = field.getAnnotation(HyperParameter.class);
							JSONObject parJSON = new JSONObject();
							parJSON.put("parameterName", hp.parameterName());
							parJSON.put("defaultValue", hp.defaultValue());
							parJSON.put("minimumValue", hp.minimumValue());
							parJSON.put("maximumValue", hp.maximumValue());
							parameters.add(parJSON);
					}
				}
				JSONObject method = new JSONObject();
				method.put("hyperParameters", parameters);
				method.put("class", c.getCanonicalName());
				cl.add(method);
			}
		}
		obj.put("methods", cl);
		return obj;
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, JSONException {
		AnnotationsInfo info = new AnnotationsInfo("es.tid.recsys.giraph.ranking");
		System.out.println(info.getInfo());
	}
}
