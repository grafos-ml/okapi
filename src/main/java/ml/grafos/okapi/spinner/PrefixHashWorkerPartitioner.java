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
package ml.grafos.okapi.spinner;

import org.apache.giraph.partition.HashWorkerPartitioner;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/*
 * expects I as PrefixIntWritable
 */
@SuppressWarnings("rawtypes")
public class PrefixHashWorkerPartitioner<I extends WritableComparable, V extends Writable, E extends Writable>
		extends HashWorkerPartitioner<I, V, E> {

	@Override
	public PartitionOwner getPartitionOwner(I vertexId) {
		PartitionedLongWritable id = (PartitionedLongWritable) vertexId;
		return partitionOwnerList.get(Math.abs(id.getPartition()
				% partitionOwnerList.size()));
	}
}
