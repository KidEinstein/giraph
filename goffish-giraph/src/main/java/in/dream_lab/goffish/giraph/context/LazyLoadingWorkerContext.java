package in.dream_lab.goffish.giraph.context;

import in.dream_lab.goffish.giraph.graph.SubgraphVertices;
import in.dream_lab.goffish.giraph.partitionstore.LoadMappingReader;
import in.dream_lab.goffish.giraph.partitionstore.SubgraphStoreLoader;
import org.apache.giraph.worker.WorkerContext;
import org.apache.log4j.Logger;
import org.python.antlr.op.Sub;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by ravikant on 16/6/17.
 */
public class LazyLoadingWorkerContext extends WorkerContext {

    private HashMap<Long,SubgraphVertices> SubgraphStore;

    final String PARTITION_LOAD_ASSIGNMENT_PATH = "giraph.loadassignment.input.path";
    final String SERIALIZED_INPUT_PATH = "giraph.serialized.input.path";

    private static Logger LOG = Logger.getLogger(LoadMappingReader.class);

    @Override
    public void preApplication() throws InstantiationException, IllegalAccessException {


        LOG.debug("CONTEXT_ID,"+getContext().getTaskAttemptID().getTaskID());// returns task_1497157325599_0057_m_000033

        String task_id=getContext().getTaskAttemptID().getTaskID().toString();

        int wid=Integer.parseInt(task_id.split("_")[4]) ;

//        int id=1;
        SubgraphStore=new HashMap<>();
        //read from hdfs
        String hdfspath = getContext().getConfiguration().get(PARTITION_LOAD_ASSIGNMENT_PATH);
//        LOG.debug("CONTEXT,PARTITION_LOAD_ASSIGNMENT_PATH,wid,"+wid+",loading pid,"+hdfspath);
//        int wid = getMyWorkerID();
        //file format wid,pid1,pid2,...
        try {
        Set<Integer> partitionsToLoad= LoadMappingReader.readFile(hdfspath,wid);


        for (Integer pid:partitionsToLoad){
            LOG.debug("CONTEXT,LOADP,wid,"+wid+",loading pid,"+pid);
        }

            hdfspath=getContext().getConfiguration().get(SERIALIZED_INPUT_PATH);

//            LOG.debug("CONTEXT,SERIALIZED_INPUT_PATH,wid,"+wid+",loading pid,"+hdfspath);


            try {
                SubgraphStoreLoader.readPartitionStore(hdfspath,partitionsToLoad,SubgraphStore);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

        }catch (IOException e){
            e.printStackTrace();
        }

        for(long sgid:SubgraphStore.keySet()){
            LOG.debug("LazyLoadingWorkerContext_PREAPP,sgid,"+sgid+",has VCOUNT,"+SubgraphStore.get(sgid).getNumVertices());
        }
    }

    public  SubgraphVertices getSubgraphStructure(long subgraphid) {
//        LOG.debug("LazyLoadingWorkerContext,querying for sgid,"+subgraphid);
//        LOG.debug("LazyLoadingWorkerContext,sgid,"+subgraphid+",has VCOUNT,"+SubgraphStore.get(subgraphid).getNumVertices());
        return SubgraphStore.get(subgraphid);
    }

    public int getNumSubgraphs(){return SubgraphStore.size();}

    public HashMap<Long, SubgraphVertices> getSubgraphStore() {
        return SubgraphStore;
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
