package in.dream_lab.goffish.giraph.partitionstore;

import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;
import in.dream_lab.goffish.giraph.graph.DefaultRemoteSubgraphVertex;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphEdge;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphVertex;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.*;
//arg : serialized filename
/**
 * Created by ravikant on 16/5/17.
 */
public class PartitionStoreReader {

    public static void main(String[] args) throws IOException {


        String filename=args[0];

        ByteBuf buffer = Unpooled.copiedBuffer(Files.readAllBytes((new File(filename)).toPath()));

        ByteBufInputStream dataInput=new ByteBufInputStream(buffer);

        SubgraphVertices v=new SubgraphVertices();

//        GiraphSubgraphConfiguration<K, I, V, E, S, J> giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(conf);

        ShortestPathSubgraphValue subgraphValue = new ShortestPathSubgraphValue();
        subgraphValue.readFields(dataInput);

        v.setSubgraphValue(subgraphValue);

        int numVertices = dataInput.readInt();
//    System.out.println("Read Subgraph Value:" + subgraphValue + "\t"+ subgraphValue.getClass().getSimpleName());
        System.out.println("Read Num Vertices:" + numVertices);
        HashMap<LongWritable, IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable>>vertices = new HashMap<>();
        for (int i = 0; i < numVertices; i++) {
            DefaultSubgraphVertex subgraphVertex = new DefaultSubgraphVertex();

            LongWritable id = new LongWritable();
            id.readFields(dataInput);
            DoubleWritable value=new DoubleWritable();
            value.readFields(dataInput);

            subgraphVertex.setId(id);
            subgraphVertex.setValue(value);

            int numEdges = dataInput.readInt();

            System.out.println("Reading vertex "+id+" has edges "+numEdges);
            LinkedList<IEdge> outEdges = Lists.newLinkedList();
            for (int j = 0; j < numEdges; j++) {

                DefaultSubgraphEdge se = new DefaultSubgraphEdge<>();
                LongWritable targetId = new LongWritable();
//                NullWritable edgeValue = new NullWritable();
                targetId.readFields(dataInput);
//                edgeValue.readFields(dataInput);
                se.initialize(null, null, targetId);

                outEdges.add(se);
                System.out.println("Reading vertex "+id+" has edge to "+targetId);
            }

            subgraphVertex.setOutEdges(outEdges);

            vertices.put((LongWritable) subgraphVertex.getVertexId(), subgraphVertex);
        }
        v.initialize(vertices);

        HashMap<LongWritable, IRemoteVertex>remoteVertices = new HashMap<>();
        int numRemoteVertices = dataInput.readInt();

        v.setRemoteVertices(remoteVertices);

        System.out.println("This subgraph has remote vertices "+numRemoteVertices);
//        for (int i = 0; i < numRemoteVertices; i++) {
//            DefaultRemoteSubgraphVertex<V, E, I, J, K> remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
//            remoteSubgraphVertex.readFields(giraphSubgraphConfiguration, dataInput);
//            remoteVertices.put(remoteSubgraphVertex.getVertexId(), remoteSubgraphVertex);
//        }

        MapWritable subgraphPartitionMapping = new MapWritable();
        subgraphPartitionMapping.readFields(dataInput);

        v.setSubgraphPartitionMapping(subgraphPartitionMapping);

        System.out.println("TEST,PartitionStore,readlocal_vertex," + v.getNumVertices()+" "+vertices.size());


    }

}
