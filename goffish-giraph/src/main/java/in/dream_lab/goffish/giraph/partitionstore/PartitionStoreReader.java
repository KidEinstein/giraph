package in.dream_lab.goffish.giraph.partitionstore;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;
import in.dream_lab.goffish.giraph.graph.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;

//import java.nio.file.Path;


//arg : serialized filename
/**
 * Created by ravikant on 16/5/17.
 */
public class PartitionStoreReader {

    public static final Logger LOG = Logger.getLogger(PartitionStore.class);

    public void readSubgraph(String filename, DefaultSubgraph sg) throws IOException, URISyntaxException {

        LOG.debug("PartitionStoreReader got argument "+filename);


//        String filename=args[0];

//        ByteBuf buffer = Unpooled.copiedBuffer(Files.readAllBytes((new File(filename)).toPath()));

        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://orion-00:9000/" + filename), conf);
//        FileStatus[] fileStatus = fs.listStatus(new Path("hdfs://orion-00:9000/" + filename));

        Path path = new Path(filename);
        FileSystem hdfs = path.getFileSystem(conf);
        ContentSummary cSummary = hdfs.getContentSummary(path);
        long length = cSummary.getLength();

        byte[] bf=new byte[(int)length];

        FSDataInputStream fsDataInputStream = hdfs.open(path);

        fsDataInputStream.read(0L,  bf, 0, (int)length);

        ByteBuf buffer = Unpooled.copiedBuffer(bf);

        ByteBufInputStream dataInput=new ByteBufInputStream(buffer);

        SubgraphVertices v=new SubgraphVertices();

//        GiraphSubgraphConfiguration<K, I, V, E, S, J> giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(conf);

        ShortestPathSubgraphValue subgraphValue = new ShortestPathSubgraphValue();
        subgraphValue.readFields(dataInput);


//        sg.setSubgraphValue(subgraphValue);

        int numVertices = dataInput.readInt();
//    LOG.debug("Read Subgraph Value:" + subgraphValue + "\t"+ subgraphValue.getClass().getSimpleName());
        LOG.debug("Read Num Vertices:" + numVertices);
        HashMap<LongWritable, IVertex<NullWritable, DoubleWritable, LongWritable, LongWritable>>vertices = new HashMap<>();
        for (int i = 0; i < numVertices; i++) {
            DefaultSubgraphVertex subgraphVertex = new DefaultSubgraphVertex();

            LongWritable id = new LongWritable();
            id.readFields(dataInput);
            NullWritable value = NullWritable.get();

            subgraphVertex.setId(id);
            subgraphVertex.setValue(value);

            int numEdges = dataInput.readInt();

            LOG.debug("Reading vertex "+id+" has edges "+numEdges);
            LinkedList<IEdge> outEdges = Lists.newLinkedList();
            for (int j = 0; j < numEdges; j++) {

                DefaultSubgraphEdge se = new DefaultSubgraphEdge<>();
                LongWritable targetId = new LongWritable();
//                NullWritable edgeValue = new NullWritable();
                targetId.readFields(dataInput);
//                edgeValue.readFields(dataInput);
                se.initialize(null, null, targetId);

                outEdges.add(se);
                LOG.debug("Reading vertex "+id+" has edge to "+targetId);
            }

            subgraphVertex.setOutEdges(outEdges);

            vertices.put((LongWritable) subgraphVertex.getVertexId(), subgraphVertex);
        }


        HashMap<LongWritable, IRemoteVertex>remoteVertices = new HashMap<>();
        int numRemoteVertices = dataInput.readInt();


        LOG.debug("This subgraph has remote vertices "+numRemoteVertices);
        for (int i = 0; i < numRemoteVertices; i++) {
            DefaultRemoteSubgraphVertex remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
            LongWritable id = new LongWritable();
            id.readFields(dataInput);
            remoteSubgraphVertex.setId(id);
            //    LOG.debug("Read: " + "Number edges: " + numEdges);
            LongWritable subgraphId = new LongWritable();
            subgraphId.readFields(dataInput);
            remoteSubgraphVertex.setSubgraphId(subgraphId);
            remoteVertices.put(id, remoteSubgraphVertex);
        }

//        v.setRemoteVertices(remoteVertices);
//        sg.getSubgraphValue()

        MapWritable subgraphPartitionMapping = new MapWritable();
        subgraphPartitionMapping.readFields(dataInput);

//        v.setSubgraphPartitionMapping(subgraphPartitionMapping);
        sg.lazyload(vertices,remoteVertices,subgraphPartitionMapping);


        LOG.debug("TEST,PartitionStore,readlocal_vertex," + sg.getVertexCount()+" "+vertices.size());

//        sg.setSubgraphValue();

        sg.setInitialized();

        LOG.debug("SGID "+sg.getSubgraphId()+" initialized in pid "+sg.getPartitionId()+" has vertices "+sg.getVertexCount());
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        PartitionStoreReader r=new PartitionStoreReader();
//        r.readSubgraph(args[0]);
    }
}
