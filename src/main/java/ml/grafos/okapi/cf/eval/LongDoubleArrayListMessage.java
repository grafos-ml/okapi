package ml.grafos.okapi.cf.eval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LongDoubleArrayListMessage implements Writable{
	long senderId;
	DoubleArrayListWritable factors;
	double score;
	
	public LongDoubleArrayListMessage() {
	}
	
	public LongDoubleArrayListMessage(LongDoubleArrayListMessage msg){
		this.senderId = msg.senderId;
		this.factors = msg.factors;
		this.score = msg.score;
	}
	
	public LongDoubleArrayListMessage(long senderId, DoubleArrayListWritable factors, double score) {
		this.senderId = senderId;
		this.factors = factors;
		this.score = score;
	}
	
	public long getSenderId() {
		return senderId;
	}
	public void setSenderId(long senderId) {
		this.senderId = senderId;
	}
	public DoubleArrayListWritable getFactors() {
		return factors;
	}
	public void setFactors(DoubleArrayListWritable factors) {
		this.factors = factors;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	public void readFields(DataInput data) throws IOException {
		senderId = data.readLong();
		factors = new DoubleArrayListWritable();
		factors.readFields(data);
		score = data.readDouble();
		
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(senderId);
		factors.write(out);
		out.writeDouble(score);
	}
}
