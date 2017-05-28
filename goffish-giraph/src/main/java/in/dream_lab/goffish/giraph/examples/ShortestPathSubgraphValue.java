package in.dream_lab.goffish.giraph.examples;

import io.netty.buffer.ByteBufOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by anirudh on 10/02/17.
 */
public class ShortestPathSubgraphValue implements Writable
{
  public Map<Long, Short> shortestDistanceMap;

  private static final Logger LOG  = Logger.getLogger(ShortestPathSubgraphValue.class);

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (shortestDistanceMap == null) {
      shortestDistanceMap = new HashMap<>();
    }

    String classname = dataOutput.getClass().toString().split(" ")[1];
    int initial_size = 0;

    if (classname.equals("io.netty.buffer.ByteBufOutputStream")) {

      LOG.info("TEST,ShortestPathSubgraphValue.write,initial_size," + ((ByteBufOutputStream) dataOutput).writtenBytes());

      initial_size = ((ByteBufOutputStream) dataOutput).writtenBytes();

      long startTime = System.currentTimeMillis();


      dataOutput.writeInt(shortestDistanceMap.size());
      for (Map.Entry<Long, Short> entry : shortestDistanceMap.entrySet()) {
        dataOutput.writeLong(entry.getKey());
        dataOutput.writeShort(entry.getValue());
      }

      LOG.info("TEST,ShortestPathSubgraphValue.write,took," + (System.currentTimeMillis() - startTime) + ",size," + (((ByteBufOutputStream) dataOutput).writtenBytes() - initial_size));

      LOG.info("TEST,ShortestPathSubgraphValue.write,debug," + ((ByteBufOutputStream) dataOutput).writtenBytes() + "," + initial_size + ",size," + (((ByteBufOutputStream) dataOutput).writtenBytes() - initial_size));

    } else {
      dataOutput.writeInt(shortestDistanceMap.size());
      for (Map.Entry<Long, Short> entry : shortestDistanceMap.entrySet()) {
        dataOutput.writeLong(entry.getKey());
        dataOutput.writeShort(entry.getValue());
      }
    }
  }
  @Override
  public void readFields(DataInput dataInput) throws IOException {

      String classname = dataInput.getClass().toString().split(" ")[1];


      if (classname.equals("io.netty.buffer.ByteBufInputStream")) {
          long startTime = System.currentTimeMillis();
          int size = dataInput.readInt();
          shortestDistanceMap = new HashMap<>(size);
          for (int i = 0; i < size; i++) {
              shortestDistanceMap.put(dataInput.readLong(), dataInput.readShort());
          }
          LOG.info("TEST,ShortestPathSubgraphValue.read,took," + (System.currentTimeMillis() - startTime));

    }else{
      int size = dataInput.readInt();
    shortestDistanceMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      shortestDistanceMap.put(dataInput.readLong(), dataInput.readShort());
    }
    }
  }
}
