package es.tid.graphlib.partitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/*
 * Author: Bamboo
 * This class provides the wrapper for the sending message.
 */

public class IntMessageWrapper implements WritableComparable<IntMessageWrapper> {
	private IntWritable sourceId;
	private IntWritable message;

	// TODO SHOULD BE STATIC RIGHT?
	// Should be actually removed!!!
	private ImmutableClassesGiraphConfiguration<IntWritable,?,?,IntWritable> conf;

	/** Constructor */
	public IntMessageWrapper(){ }

	public IntMessageWrapper(IntWritable sourceId, IntWritable message){
		this.sourceId      = sourceId;
		this.message       = message;
	}

	/** Source Id */
	public IntWritable getSourceId() {
		return sourceId;
	}

	public void setSourceId(IntWritable sourceId) {
		this.sourceId = sourceId;
	}

	/** Message */
	public IntWritable getMessage() {
		return message;
	}

	public void setMessage(IntWritable message) {
		this.message = message;
	}

	public ImmutableClassesGiraphConfiguration<IntWritable,?,?,IntWritable> getConf() {
		return conf;
	}

	public void setConf(ImmutableClassesGiraphConfiguration<IntWritable,?,?,IntWritable> conf) {
		this.conf = conf;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		sourceId = new IntWritable();
		toString();
		sourceId.readFields(input);
		message = new IntWritable();
		message.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {

		if(sourceId == null) 
			throw new IllegalStateException("write: Null destination vertex index");

		sourceId.write(output);
		message.write(output);
	}

	@Override
	public String toString() {
		return "MessageWrapper{" +
				", sourceId=" + sourceId +
				", message=" + message +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		IntMessageWrapper that = (IntMessageWrapper) o;

		if (message != null ? !message.equals(that.message) : that.message != null) return false;
		if (sourceId != null ? !sourceId.equals(that.sourceId) : that.sourceId != null) return false;
		return true;

	}

	@Override
	public int compareTo(IntMessageWrapper wrapper) {

		if (this == wrapper )
			return 0;

		if (this.sourceId.compareTo(wrapper.getSourceId()) == 0)
			return this.message.compareTo(wrapper.getMessage());
		else
			return this.sourceId.compareTo(wrapper.getSourceId());
	}
}