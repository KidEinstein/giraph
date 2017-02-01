package org.apache.giraph.comm.messages;

import org.apache.giraph.graph.SubgraphId;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by anirudh on 28/11/16.
 */
public class SubgraphMessage<S extends WritableComparable, M extends Writable> implements Writable {

  public static final Logger LOG = Logger.getLogger(SubgraphMessage.class);

  private S subgraphId;
  private M message;

  public SubgraphMessage() {

  }

  public S getSubgraphId() {
    return subgraphId;
  }

  public M getMessage() {
    return message;
  }

  public SubgraphMessage(S subgraphId, M message) {
    this.subgraphId = subgraphId;
    this.message = message;
  }
  public void initializeSubgraphId(S subgraphId) {
    this.subgraphId = subgraphId;
  }

  public void initializeMessageValue(M messageValue) {
    this.message = messageValue;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    //LOG.info("Writing message, ID: " + subgraphId + " Value: " + message);
    subgraphId.write(dataOutput);
    message.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    subgraphId.readFields(dataInput);
    message.readFields(dataInput);
  }


}
