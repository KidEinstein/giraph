package in.dream_lab.goffish.giraph.partitionstore;

import in.dream_lab.goffish.giraph.graph.DefaultSubgraph;
import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;
import org.python.antlr.op.In;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ravikant on 14/6/17.
 *
 * Args
 * 1. PartitionID
 * 2. Store directory in hdfs where partitions are stored in serialized format
 */
class SubgraphLoader implements Runnable {

    private Object lock= new Object();
    private static List<String> listoffiles;
    SubgraphVertices sgv;

    private int id;

//    public void setSg(DefaultSubgraph sg) {
//        this.sgv = sg;
//    }

    public static void setListoffiles(List<String> listoffiles) {
        SubgraphLoader.listoffiles = listoffiles;
    }

    public SubgraphLoader(int id,SubgraphVertices sg) {
        this.id = id;
        this.sgv=sg;
    }

    @Override
    public void run() {

        String filename=getNextFile(listoffiles);
//        LOG.debug("Thread : "+id+" processing "+filename+" by "+Thread.currentThread().getId() );
        PartitionStoreReader r=new PartitionStoreReader();
        try {
            try {
                r.readSubgraphValue(filename,sgv); //need an equivalent of readSubgraphValue
//                sg.isInitialized=true;
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    String getNextFile(List<String> listoffiles){
        String file;
        synchronized (lock){

            file=listoffiles.get(0);
            listoffiles.remove(0);
        }
        return file;
    }
}

public class SubgraphStoreLoader {

    public static final Logger LOG = Logger.getLogger(SubgraphStoreLoader.class);

    //serialized files naming convention : pid_sgid.ser
    /*
    * Argument
    *@directory hdfs directory where all serialized files are located
    *@ partition set of partitions which are to be loaded
    */
    List<String> getListofFilesFromhdfs(String directory,Set<Integer>partitions) throws URISyntaxException, IOException, FileNotFoundException {
        LOG.debug("Inputloader Listing files:");
        ArrayList<String> listoffiles=new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI( directory), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(directory));
        for(FileStatus status : fileStatus){
            LOG.debug("File: "+status.getPath().toString());
            System.out.println("File: "+status.getPath().toString());
//            e.g.hdfs://orion-00:9000/user/bduser/lazy_loading/ORKT-40/8589934593.ser
            String[] fullpath = status.getPath().toString().split("/");
            String filename = fullpath[fullpath.length-1];

            int pid= Integer.parseInt(filename.split("_")[0]);

            if(partitions.contains(pid))
                listoffiles.add(status.getPath().toString());
        }
        return listoffiles;
    }

    //Arguments :
//    1. base directory
//    2. HashMap
//    3. Set of partitions
    public static void readPartitionStore(String directory, Set<Integer>partitions, HashMap<Long,SubgraphVertices>SubgraphStore) throws InterruptedException, IOException, URISyntaxException {


        InputLoader il=new InputLoader();

//        List<String> l=il.getListOfFiles(directory);
        List<String> l=il.getListofFilesFromhdfs(directory);
//        for(String p: l.getListOfFiles(directory)){
//            LOG.debug(p);
//        }

        SubgraphLoader.setListoffiles(l);

        ExecutorService executor= Executors.newFixedThreadPool(4);

        LOG.debug("Number of files "+l.size());
//        LOG.debug();

        //form the subgraph object here
        for(int i=0;i<l.size();i++){


            String[] fullpath = l.get(i).split("/");
            String filename = fullpath[fullpath.length-1];

//            int pid= Integer.parseInt(filename.split("_")[0]);
            LOG.debug("SubgraphStore,filename,"+filename);

//            if(filename.split("_").length!=0) {

//                LOG.debug((filename.split("_")[1]));
//                String temp=(filename.split("_")[1]);
//                LOG.debug(temp);
                long sgid = Long.parseLong((filename.split("_")[1]).split("\\.")[0]);

                SubgraphVertices sgv = new SubgraphVertices();

                SubgraphStore.put(sgid, sgv);

                executor.submit(new SubgraphLoader(i, sgv));
//            }else{
//                LOG.debug("SubgraphStore,FAILfilename,"+filename);
//            }
        }
        executor.shutdown();
        LOG.debug("EXECUTOR_SERVICE_SHUTDOWN");
        executor.awaitTermination(1, TimeUnit.HOURS);
        LOG.debug("EXECUTOR_SERVICE_TERMINATE");

    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        SubgraphStoreLoader sl= new SubgraphStoreLoader();
        String dirname="hdfs://orion-00:9000/user/bduser/lazy_loading/ORKT-40/";
        Set<Integer>pids=new HashSet<>();
        sl.getListofFilesFromhdfs(dirname,pids);
    }


}
