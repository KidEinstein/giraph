package in.dream_lab.goffish.giraph.partitionstore;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * Created by ravikant on 1/2/17.
 * file format wid,pid1,pid2,...
 */

//FIXME: SUperstep number for giraph and goffish is same
public class LoadMappingReader {

    private static Logger LOG = Logger.getLogger(LoadMappingReader.class);

    public static Set<Integer> readFile(String file, int worker_id) throws IOException {

        String cvsSplitBy = ",";
        Set<Integer>loadSet=new HashSet<>();
        //read this as custom argument

        Configuration conf = new Configuration();
        Path pt = new Path(file);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;

        line = br.readLine();

        //input format partitionID,superstep,workerID
        while (line != null) {
            if(line.trim().isEmpty()){
                line = br.readLine();
                continue;
            }
            String[] entry = line.split(cvsSplitBy);
            int wid =( Integer.parseInt(entry[0])+1); //FIXME: in MappingReader we have done +1 for wid as script starts vmid from 0 but task 0 is master
            if(wid==worker_id){

                for(int i=1;i<entry.length;i++){

                    loadSet.add(Integer.parseInt(entry[i]));
                }

                break;
            }

            line = br.readLine();
        }


        return loadSet;
    }
}