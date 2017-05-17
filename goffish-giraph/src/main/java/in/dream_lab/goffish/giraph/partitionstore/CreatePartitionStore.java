package in.dream_lab.goffish.giraph.partitionstore;

import org.apache.commons.collections.map.*;
import com.google.common.collect.Lists;
import in.dream_lab.goffish.api.IEdge;
//import in.dream_lab.goffish.giraph.conf.GiraphSubgraphConfiguration;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphEdge;
import in.dream_lab.goffish.giraph.graph.DefaultSubgraphVertex;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;

import in.dream_lab.goffish.api.IVertex;
import in.dream_lab.goffish.giraph.examples.ShortestPathSubgraphValue;

import org.apache.hadoop.io.*;

/**
 * Created by ravikant on 15/5/17.
 */
//input is a folder which has files for all subgraphs
public class CreatePartitionStore {

    static IEdge<NullWritable, LongWritable, NullWritable> decodeVertexEdge(String id) {
        LongWritable vertexId = new LongWritable(Long.parseLong(id));
        DefaultSubgraphEdge<LongWritable, NullWritable, NullWritable> subgraphEdge = new DefaultSubgraphEdge<>();
        subgraphEdge.initialize(NullWritable.get(), NullWritable.get(), vertexId);
        return subgraphEdge;
    }

     static LinkedList<IEdge> getVertexEdges(String[] values) throws
            IOException {
        int i = 2;
        LinkedList<IEdge> edges = Lists.newLinkedList();
        while (i < values.length) {
            edges.add(decodeVertexEdge(values[i]));
            i += 1;
        }
        return edges;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(System.getProperty("java.class.path"));

        File folder = new File(args[0]);
        String graphName=args[1];
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            File file = listOfFiles[i];
            if (file.isFile() && file.getName().endsWith(".txt")) {
                //create datastructures
                SubgraphVertices<Writable, DoubleWritable, DoubleWritable, LongWritable, LongWritable, LongWritable> subgraphVertices = new SubgraphVertices();
                HashMap<LongWritable, IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable>> subgraphVerticesMap = new HashMap<>();


                // Open the file
                FileInputStream fstream = new FileInputStream(file.getName());
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

                String strLine;

                //get subgraphID
                LongWritable sgid =new LongWritable(Long.parseLong(br.readLine().split("\t")[0]));
                //Read File Line By Line



                while ((strLine = br.readLine()) != null)   {
                    // Print the content on the console
                    System.out.println (strLine);
                    String[] processedLine = strLine.split("\t");
                    DefaultSubgraphVertex sVertex = new DefaultSubgraphVertex();
                    sVertex.initialize(new LongWritable(Long.parseLong(processedLine[0])), new DoubleWritable(Double.parseDouble(processedLine[1])), getVertexEdges(processedLine));
                    IVertex<DoubleWritable, DoubleWritable, LongWritable, LongWritable> subgraphVertex=sVertex;

                    subgraphVerticesMap.put(subgraphVertex.getVertexId(), subgraphVertex);


                }
                subgraphVertices.initialize(subgraphVerticesMap);
//                GiraphSubgraphConfiguration giraphSubgraphConfiguration = new GiraphSubgraphConfiguration(getConf());
                subgraphVertices.setSubgraphValue(new ShortestPathSubgraphValue() );

                //Close the input stream
                br.close();
//
//                try (Output output = new Output(new FileOutputStream("KryoTest.ser"))) {
//                    Kryo kryo=new Kryo();
//                    kryo.writeClassAndObject(output, subgraphVertices);
//                } catch (FileNotFoundException ex) {
//                    Logger.getLogger(KryTest.class.getName()).log(Level.SEVERE, null, ex);
//                }

                File outfile = new File(sgid+".ser");
                FileOutputStream fop = new FileOutputStream(outfile);
                if (!file.exists()) {
                    file.createNewFile();
                }

//                io.netty.buffer.UnpooledHeapByteBuf
//                UnpooledHeapByteBuf buffer;
//                buffer = new UnpooledHeapByteBuf(new UnpooledByteBufAllocator(true),0,1024);
                ByteBuf byteBuf = Unpooled.buffer(0);
                ByteBufOutputStream b= new ByteBufOutputStream(byteBuf);
                subgraphVertices.write(b);

                fop.write(b.buffer().array());
                fop.flush();
                fop.close();
            }
        }


    }
}
