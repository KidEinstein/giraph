package in.dream_lab.goffish.giraph.partitionstore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by ravikant on 25/5/17.
 */
//input is directory for partition-store which has separate file for each subgraph

class Loader implements Runnable {

    private Object lock= new Object();
    private static List<String> listoffiles;

    private int id;

    public static void setListoffiles(List<String> listoffiles) {
        Loader.listoffiles = listoffiles;
    }

    public Loader(int id) {
        this.id = id;
    }

    @Override
    public void run() {

        System.out.println("Thread : "+id+" processing "+getNextFile(listoffiles) );
        try {
            if(id%2==0)
            Thread.sleep(1500);
            else
                Thread.sleep(500);
        } catch (InterruptedException e) {
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

public class InputLoader{


    List<String> getListOfFiles(String directory){

        ArrayList<String> listoffiles=new ArrayList<>();
        File[] files = new File(directory).listFiles();
        for(File file : files){
            if(file.isFile()){
//                System.out.println(file.getAbsolutePath());
                listoffiles.add((file.getAbsolutePath().toString()));
            }
        }

        return listoffiles;
    }

    public static void main(String[] args) throws InterruptedException {

        Scanner in= new Scanner(System.in);
        String  directory = in.nextLine();

        InputLoader il=new InputLoader();

        List<String> l=il.getListOfFiles(directory);
//        for(String p: l.getListOfFiles(directory)){
//            System.out.println(p);
//        }

        Loader.setListoffiles(l);

        ExecutorService executor= Executors.newFixedThreadPool(2);

        System.out.println("Number of files "+l.size());

        for(int i=0;i<l.size();i++){
            executor.submit(new Loader(i));
        }
        executor.shutdown();

        executor.awaitTermination(1, TimeUnit.DAYS);

    }
}