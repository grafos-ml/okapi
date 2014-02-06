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
package ml.grafos.okapi.cf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.hadoop.io.Writable;

/**
 * Messages send in most of the CF algorithm typically must carry the id of the
 * message sender as well as the payload of the message, that is, the latent
 * vector.
 * @author dl
 *
 */
public class FloatMatrixMessage implements Writable {
  CfLongId senderId;
  FloatMatrixWritable factors;
  float score;

  public FloatMatrixMessage() {
  }

  public FloatMatrixMessage(FloatMatrixMessage msg) {
    this.senderId = msg.senderId;
    this.factors = msg.factors;
    this.score = msg.score;
  }

  public FloatMatrixMessage(CfLongId senderId, FloatMatrixWritable factors,
      float score) {
    this.senderId = senderId;
    this.factors = factors;
    this.score = score;
  }

  public CfLongId getSenderId() {
    return senderId;
  }

  public void setSenderId(CfLongId senderId) {
    this.senderId = senderId;
  }

  public FloatMatrixWritable getFactors() {
    return factors;
  }

  public void setFactors(FloatMatrixWritable factors) {
    this.factors = factors;
  }

  public float getScore() {
    return score;
  }

  public void setScore(float score) {
    this.score = score;
  }

  public void readFields(DataInput input) throws IOException {
    senderId = new CfLongId(); 
    senderId.readFields(input);  
    factors = new FloatMatrixWritable();
    factors.readFields(input);
    score = input.readFloat();

  }

  public void write(DataOutput output) throws IOException {
    senderId.write(output);
    factors.write(output);
    output.writeFloat(score);
  }
  
  @Override
  public boolean equals(Object matrix) {
    if (matrix==null) {
      return false;
    }
    FloatMatrixMessage other = (FloatMatrixMessage) matrix;
    if (senderId==null && other.senderId!=null) {
      return false;
    } else if (!senderId.equals(other.senderId)) {
      return false;
    }
    if (factors==null && other.factors!=null) {
      return false;
    } else if (!factors.equals(other.factors)) {
      return false;
    }
    if (score!=other.score) {
      return false;
    }
    return true;
  }
  
  @Override
  public String toString() {
    return "["+senderId+"] "+score+" "+factors;
  }
}
