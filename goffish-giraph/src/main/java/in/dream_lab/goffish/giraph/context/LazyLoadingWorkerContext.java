package in.dream_lab.goffish.giraph.context;

import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.partitionstore.LoadMappingReader;
import in.dream_lab.goffish.giraph.partitionstore.SubgraphStoreLoader;
import org.apache.giraph.worker.WorkerContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ravikant on 16/6/17.
 */
public class LazyLoadingWorkerContext extends WorkerContext {

    private Map<Long,SubgraphVertices> SubgraphStore;

    final String PARTITION_LOAD_ASSIGNMENT_PATH = "giraph.loadassignment.input.path";
    final String SERIALIZED_INPUT_PATH = "giraph.serialized.input.path";

    private static Logger LOG = Logger.getLogger(LoadMappingReader.class);

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {


        LOG.debug("CONTEXT_ID,"+getContext().getTaskAttemptID().getTaskID());// returns task_1497157325599_0057_m_000033
        int id=1;
        SubgraphStore=new HashMap<>();
        //read from hdfs
        String hdfspath = getContext().getConfiguration().get(PARTITION_LOAD_ASSIGNMENT_PATH);
        LOG.debug("CONTEXT,PARTITION_LOAD_ASSIGNMENT_PATH,wid,"+id+",loading pid,"+hdfspath);
//        int wid = getMyWorkerID();
        //file format wid,pid1,pid2,...
        try {
        Set<Integer> partitionsToLoad= LoadMappingReader.readFile(hdfspath,id);


        for (Integer pid:partitionsToLoad){
            LOG.debug("CONTEXT,LOADP,wid,"+id+",loading pid,"+pid);
        }

            hdfspath=getContext().getConfiguration().get(SERIALIZED_INPUT_PATH);

            LOG.debug("CONTEXT,SERIALIZED_INPUT_PATH,wid,"+id+",loading pid,"+hdfspath);


//           SubgraphStoreLoader.readPartitionStore(hdfspath,partitionsToLoad,SubgraphStore);

        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public  SubgraphVertices getSubgraphStructure(long subgraphid) {
        return SubgraphStore.get(subgraphid);
    }

    @Override

    public void postApplication() {

    }

    @Override
    public void preSuperstep() {

    }

    @Override
    public void postSuperstep() {

    }
}
