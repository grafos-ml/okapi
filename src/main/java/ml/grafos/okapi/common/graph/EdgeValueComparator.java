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
package ml.grafos.okapi.common.graph;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("rawtypes")
public class EdgeValueComparator<K extends Comparable,V extends Comparable> 
	implements Comparator<Map.Entry<K,V>> {

	@SuppressWarnings("unchecked")
	@Override
	public int compare(Entry<K, V> e1, Entry<K, V> e2) {
		 int res = e1.getValue().compareTo(e2.getValue());
		 if (res == 0) { 
			// equal values, compare vertex ids
			return e1.getKey().compareTo(e2.getKey()); 
		 }
		 else {
			 return res;
		 }
	}

	
}