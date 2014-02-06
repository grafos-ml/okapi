/**
 * Copyright 2013 Grafos.ml
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
package ml.grafos.okapi.spinner;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.GraphPartitionerFactory;
import org.apache.giraph.partition.HashMasterPartitioner;
import org.apache.giraph.partition.MasterGraphPartitioner;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * set it through giraph.graphPartitionerFactoryClass
 * expects I as PrefixIntWritable
 */
@SuppressWarnings("rawtypes")
public class PrefixHashPartitionerFactory<I extends WritableComparable, V extends Writable, E extends Writable>
		implements GraphPartitionerFactory<I, V, E> {
	/** Saved configuration */
	private ImmutableClassesGiraphConfiguration conf;

	@Override
	public MasterGraphPartitioner<I, V, E> createMasterGraphPartitioner() {
		return new HashMasterPartitioner<I, V, E>(getConf());
	}

	@Override
	public WorkerGraphPartitioner<I, V, E> createWorkerGraphPartitioner() {
		return new PrefixHashWorkerPartitioner<I, V, E>();
	}

	@Override
	public ImmutableClassesGiraphConfiguration getConf() {
		return conf;
	}

	@Override
	public void setConf(ImmutableClassesGiraphConfiguration conf) {
		this.conf = conf;
	}
}
