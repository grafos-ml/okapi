package es.tid.graphlib.examples;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import es.tid.graphlib.sgd.DoubleArrayListWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * Author: Bamboo
 * This class provides the wrapper for the sending message.
 */

public class MessageWrapper implements WritableComparable<MessageWrapper> {
	private IntWritable sourceId;
	private DoubleArrayListWritable message;

	// TODO SHOULD BE STATIC RIGHT?
	// Should be actually removed!!!
	private ImmutableClassesGiraphConfiguration<IntWritable,?,?,DoubleArrayListWritable> conf;

	/** Constructor */
	public MessageWrapper(){ }

	public MessageWrapper(IntWritable sourceId, DoubleArrayListWritable message){
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
	public DoubleArrayListWritable getMessage() {
		return message;
	}

	public void setMessage(DoubleArrayListWritable message) {
		this.message = message;
	}

	public ImmutableClassesGiraphConfiguration<IntWritable,?,?,DoubleArrayListWritable> getConf() {
		return conf;
	}

	public void setConf(ImmutableClassesGiraphConfiguration<IntWritable,?,?,DoubleArrayListWritable> conf) {
		this.conf = conf;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		sourceId = new IntWritable();
		//sourceId = conf.createVertexId();
		toString();
		sourceId.readFields(input);
		//message = conf.createMessageValue();
		message = new DoubleArrayListWritable();
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

		MessageWrapper that = (MessageWrapper) o;

		if (message != null ? !message.equals(that.message) : that.message != null) return false;
		if (sourceId != null ? !sourceId.equals(that.sourceId) : that.sourceId != null) return false;
		return true;

	}

	@Override
	public int compareTo(MessageWrapper wrapper) {

		if (this == wrapper )
			return 0;

		if (this.sourceId.compareTo(wrapper.getSourceId()) == 0)
			return this.message.compareTo(wrapper.getMessage());
		else
			return this.sourceId.compareTo(wrapper.getSourceId());
	}
}