package in.dream_lab.goffish.giraph.partitionstore;

import in.dream_lab.goffish.api.IRemoteVertex;
import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.formats.FlatTextSubgraphInputFormat;
import in.dream_lab.goffish.giraph.graph.*;
import org.apache.commons.collections.map.*;
import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
//import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.util.*;

import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;

import javax.validation.constraints.Null;
//input is a folder which has files for all subgraphs
/**
 * Created by ravikant on 15/5/17.
 */

public class CreatePartitionStore {

    public static void main(String[] args) throws IOException {
        HashMap<LongWritable, SubgraphInputImpl> inputSubgraphs = new HashMap<>();

//        System.out.println(System.getProperty("java.class.path"));

        File folder = new File(args[0]);
        String graphName=args[1];
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            inputSubgraphs.clear();
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".txt")) {
                // Open the file
                System.out.println("Processing file "+file.getName());
                FileInputStream fstream = new FileInputStream(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

                String strLine;
                //Read File Line By Line
                while ((strLine = br.readLine()) != null) {

                    String [] values = strLine.split("\t");

                    //get subgraphID
                    LongWritable sgid = new LongWritable(Long.parseLong(values[2]));

                    SubgraphId<LongWritable> subgraphId = new SubgraphId<>(sgid, Integer.parseInt(values[0]));

                    // Print the content on the console
//                    System.out.println (strLine);

                    SubgraphInputImpl subgraphInput;

                    if (inputSubgraphs.containsKey(sgid)) {
                        subgraphInput = inputSubgraphs.get(sgid);
                    } else {
                        subgraphInput = new SubgraphInputImpl(subgraphId);
                        inputSubgraphs.put(sgid, subgraphInput);
                    }

                    subgraphInput.addEntry(values);



//                    DefaultSubgraphVertex sVertex = new DefaultSubgraphVertex();
//                    sVertex.initialize(new LongWritable(Long.parseLong(processedLine[0])), new DoubleWritable(Double.parseDouble(processedLine[1])), getVertexEdges(processedLine));
//                    IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable> subgraphVertex=sVertex;
//
//                    subgraphVerticesMap.put(subgraphVertex.getVertexId(), subgraphVertex);


                }
//                subgraphVertices.initialize(subgraphVerticesMap);
////                GiraphSubgraphConfiguration giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(getConf());
//                subgraphVertices.setSubgraphValue(new ShortestPathSubgraphValue() );

                //Close the input stream
                br.close();
//
//                try (Output output = new Output(new FileOutputStream("KryoTest.ser"))) {
//                    Kryo kryo=new Kryo();
//                    kryo.writeClassAndObject(output, subgraphVertices);
//                } catch (FileNotFoundException ex) {
//                    Logger.getLogger(KryTest.class.getName()).log(Level.SEVERE, null, ex);
//                }

                writeSubgraphs(inputSubgraphs);
//                File outfile = new File(sgid+".ser");
//                FileOutputStream fop = new FileOutputStream(outfile);
//                if (!file.exists()) {
//                    file.createNewFile();
//                }
//
////                io.netty.buffer.UnpooledHeapByteBuf
////                UnpooledHeapByteBuf buffer;
////                buffer = new UnpooledHeapByteBuf(new UnpooledByteBufAllocator(true),0,1024);
//                ByteBuf byteBuf = Unpooled.buffer(0);
//                ByteBufOutputStream b= new ByteBufOutputStream(byteBuf);
//                subgraphVertices.write(b);
//
//                fop.write(b.buffer().array());
//                fop.flush();
//                fop.close();
            }
        }


    }

    private static void writeSubgraphs(HashMap<LongWritable, SubgraphInputImpl> inputSubgraphs) throws IOException {
        for (Map.Entry<LongWritable, SubgraphInputImpl> entry : inputSubgraphs.entrySet()) {
            File outfile = new File(entry.getValue().getSubgraphId().getPartitionId()+"_"+entry.getKey() + ".ser");//TODO:change this to have pid+sgid

            System.out.println("SGID,"+entry.getValue().getSubgraphId().getSubgraphId()+",pid,"+entry.getValue().getSubgraphId().getPartitionId()+",has VCOUNT,"+entry.getValue().getVertexLocalCount());

            FileOutputStream fop = new FileOutputStream(outfile);

//                io.netty.buffer.UnpooledHeapByteBuf
//                UnpooledHeapByteBuf buffer;
//                buffer = new UnpooledHeapByteBuf(new UnpooledByteBufAllocator(true),0,1024);
            ByteBuf byteBuf = Unpooled.buffer(0);
            ByteBufOutputStream b= new ByteBufOutputStream(byteBuf);
            entry.getValue().getSubgraphVertices().write(b);

            fop.write(b.buffer().array());
            fop.flush();
            fop.close();
        }

    }

    protected static abstract class SubgraphInput<S extends Writable, V extends Writable,
        E extends Writable, I extends WritableComparable, J extends WritableComparable, K extends WritableComparable> {

        private SubgraphId<K> subgraphId;

        private HashSet<SubgraphId<K>> neighboringSubgraphs;

        private HashMap<I, IVertex<V, E, I, J>> vertices;

        private HashMap<I, IRemoteVertex<V, E, I, J, K>> remoteVertices;

        public SubgraphId<K> getSubgraphId() {
            return subgraphId;
        }

        public int getVertexLocalCount() {
            return vertices.size();

        }

        public SubgraphInput(SubgraphId<K> subgraphId) {
            this.subgraphId = subgraphId;
            vertices = new HashMap<>( );
            remoteVertices = new HashMap<>();

            neighboringSubgraphs = new HashSet<>();
        }

        public abstract I decodeId(String s);

        public abstract K decodeSId(String s);

        public void addEntry(String[] values) {
            DefaultSubgraphVertex<V, E, I, J> subgraphVertex = new DefaultSubgraphVertex();
            subgraphVertex.initialize(decodeId(values[1]), getSubgraphVertexValue(), getVertexEdges(values));
            vertices.put(subgraphVertex.getId(), subgraphVertex);
        }

        public abstract V getSubgraphVertexValue();

        /**
         * Decode an edge from the line into an instance of a correctly typed Edge
         *
         * @param vertexId The edge's id from the line
         * @return Edge with given target id and value
         */
        public IEdge createVertexEdge(I vertexId) {
            DefaultSubgraphEdge<I, NullWritable, NullWritable> subgraphEdge = new DefaultSubgraphEdge<>();
            subgraphEdge.initialize(NullWritable.get(), NullWritable.get(), vertexId);
            return subgraphEdge;
        }

        protected LinkedList<IEdge<E, I, J>> getVertexEdges(String[] values) {
            int i = 3;
            LinkedList<IEdge<E, I, J>> edges = Lists.newLinkedList();
            while (i < values.length) {
                I vertexId = decodeId(values[i]);
                IEdge<E, I, J> e = createVertexEdge(vertexId);
                edges.add(e);
                K remoteSId = decodeSId(values[i + 1]);
                int remotePId = Integer.parseInt(values[i + 2]);
//                System.out.println("SID: " + subgraphId.getSubgraphId() + " Vertex ID " + values[1] + " Sink Vertex ID" + e.getSinkVertexId() + ",RSID:" + remoteSId + ",RPID:" + remotePId);
                if (!subgraphId.getSubgraphId().equals(remoteSId)) {
                    neighboringSubgraphs.add(new SubgraphId<>(remoteSId, remotePId));
                    DefaultRemoteSubgraphVertex<V, E, I, J, K> remoteSubgraphVertex = new DefaultRemoteSubgraphVertex<>();
                    remoteSubgraphVertex.setSubgraphId(remoteSId);
                    remoteSubgraphVertex.setId(vertexId);
                    remoteVertices.put(e.getSinkVertexId(), remoteSubgraphVertex);
                }
                i += 3;
            }
            return edges;
        }

        protected SubgraphVertices<S, V, E, I, J, K> getSubgraphVertices() {
            SubgraphVertices<S, V, E, I, J, K> subgraphVertices = new SubgraphVertices<>();
            subgraphVertices.setRemoteVertices(remoteVertices);
            subgraphVertices.initialize(vertices);
            subgraphVertices.setSubgraphValue((S) new ShortestPathSubgraphValue());
            subgraphVertices.setSubgraphPartitionMapping(getSubgraphPartitionMapping());
            return subgraphVertices;
        }

        protected MapWritable getSubgraphPartitionMapping() {
            MapWritable subgraphPartitionMapping = new MapWritable();
            for (SubgraphId<K> subgraphId : neighboringSubgraphs) {
                subgraphPartitionMapping.put(subgraphId.getSubgraphId(), new IntWritable(subgraphId.getPartitionId()));
            }
            return subgraphPartitionMapping;
        }

        protected Iterable<Edge<SubgraphId<K>, NullWritable>> getSubgraphNeighbors() {
            List<Edge<SubgraphId<K>, NullWritable>> edges = Lists.newLinkedList();
            for (SubgraphId<K> sId: neighboringSubgraphs){
                edges.add(EdgeFactory.create(sId, NullWritable.get()));
            }
            return edges;
        }

    }

    protected static class SubgraphInputImpl extends SubgraphInput<NullWritable, NullWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> {


        public SubgraphInputImpl(SubgraphId<LongWritable> subgraphId) {
            super(subgraphId);
        }

        @Override
        public LongWritable decodeId(String s) {
            return new LongWritable(Long.parseLong(s));
        }

        @Override
        public LongWritable decodeSId(String s) {
            return new LongWritable(Long.parseLong(s));
        }

        @Override
        public NullWritable getSubgraphVertexValue() {
            return NullWritable.get();
        }

    }
}
