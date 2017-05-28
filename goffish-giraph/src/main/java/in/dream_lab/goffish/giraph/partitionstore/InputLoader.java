
package in.dream_lab.goffish.giraph.partitionstore;

import in.dream_lab.goffish.giraph.graph.DefaultSubgraph;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.io.FileNotFoundException;
/**
 * Created by ravikant on 25/5/17.
 */
//input is directory for partition-store which has separate file for each subgraph

class Loader implements Runnable {

    private Object lock= new Object();
    private static List<String> listoffiles;
    DefaultSubgraph sg;

    private int id;

    public void setSg(DefaultSubgraph sg) {
        this.sg = sg;
    }

    public static void setListoffiles(List<String> listoffiles) {
        Loader.listoffiles = listoffiles;
    }

    public Loader(int id,DefaultSubgraph sg) {
        this.id = id;
        this.sg=sg;
    }

    @Override
    public void run() {

        String filename=getNextFile(listoffiles);
//        LOG.debug("Thread : "+id+" processing "+filename+" by "+Thread.currentThread().getId() );
        PartitionStoreReader r=new PartitionStoreReader();
        try {
            try {
                r.readSubgraph(filename,sg);
//                sg.isInitialized=true;
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        try {
//            if(id%2==0)
//                Thread.sleep(1500);
//            else
//                Thread.sleep(500);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
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

public class InputLoader{

    public static final Logger LOG = Logger.getLogger(InputLoader.class);

    List<String> getListOfFiles(String directory){

        ArrayList<String> listoffiles=new ArrayList<>();
        File[] files = new File(directory).listFiles();
        for(File file : files){
            if(file.isFile()){
//                LOG.debug(file.getAbsolutePath());
                listoffiles.add((file.getAbsolutePath().toString()));
            }
        }

        return listoffiles;
    }

    List<String> getListofFilesFromhdfs(String directory) throws URISyntaxException, IOException, FileNotFoundException {
        LOG.debug("Inputloader Listing files:");
        ArrayList<String> listoffiles=new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://orion-00:9000/" + directory), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(directory));
        for(FileStatus status : fileStatus){
            LOG.debug("File: "+status.getPath().toString());
            listoffiles.add(status.getPath().toString());
        }
        return listoffiles;
    }

    //TODO: read from hdfs
    public void readPartitionStore(String directory, DefaultSubgraph sg) throws InterruptedException, IOException, URISyntaxException {


        InputLoader il=new InputLoader();

//        List<String> l=il.getListOfFiles(directory);
        List<String> l=il.getListofFilesFromhdfs(directory);
//        for(String p: l.getListOfFiles(directory)){
//            LOG.debug(p);
//        }

        Loader.setListoffiles(l);

        ExecutorService executor= Executors.newFixedThreadPool(2);

        LOG.debug("Number of files "+l.size());
//        LOG.debug();

        for(int i=0;i<l.size();i++){
            executor.submit(new Loader(i,sg));
        }
        executor.shutdown();

        executor.awaitTermination(1, TimeUnit.DAYS);


    }


    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {

//        Scanner in= new Scanner(System.in);
        String  directory = args[0];

        InputLoader il=new InputLoader();

//        List<String> l=il.getListOfFiles(directory);

        List<String> l=il.getListofFilesFromhdfs(directory);
//        for(String p: l.getListOfFiles(directory)){
//            LOG.debug(p);
//        }

        Loader.setListoffiles(l);

        ExecutorService executor= Executors.newFixedThreadPool(2);

        LOG.debug("Number of files "+l.size());

        for(int i=0;i<l.size();i++){
            //requires defaultsubgraph object to load
//            executor.submit(new Loader(i));
        }
        executor.shutdown();

        LOG.debug("DONE processing");
//        executor.awaitTermination(1, TimeUnit.DAYS);

    }
}