package org.apache.giraph.graph.migration;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.StrConfOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by ravikant on 1/2/17.
 */

//TODO: return a table of mapping
public class MappingReader {
  public static final StrConfOption MAPPING_FILE = new StrConfOption("mappingFile", null, "Mapping file");
  private static Logger LOG = Logger.getLogger(MappingReader.class);

  public static Map<Integer, Map<Integer, Set<Integer>>> readFile(ImmutableClassesGiraphConfiguration conf) throws IOException {

    String cvsSplitBy = ",";
    HashMap<Integer, Map<Integer, Set<Integer>>> mapping = new HashMap<>();
    //read this as custom argument
    String file = MAPPING_FILE.get(conf);
    Path pt = new Path(file);
    FileSystem fs = FileSystem.get(conf);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;

    line = br.readLine();
    //input format superstep,partition,time
    while (line != null) {
      String[] entry = line.split(cvsSplitBy);
      int partitionId = Integer.parseInt(entry[0]);
      int superStep =  Integer.parseInt(entry[1]);
      int workedId =  Integer.parseInt(entry[2]);
      if (mapping.containsKey(superStep)) {
        Map<Integer, Set<Integer>> widPidMap = mapping.get(superStep);
        if (widPidMap.containsKey(workedId)) {
          Set<Integer> pidSet = widPidMap.get(workedId);
          pidSet.add(partitionId);
        } else {
          HashSet<Integer> pidSet = new HashSet<>();
          pidSet.add(partitionId);
          widPidMap.put(workedId, pidSet);
        }
      } else {
        HashMap<Integer, Set<Integer>> widPidMap = new HashMap<>();
        mapping.put(superStep, widPidMap);
        HashSet<Integer> pidSet = new HashSet<>();
        pidSet.add(partitionId);
        widPidMap.put(workedId, pidSet);
      }
      line = br.readLine();
    }


    return mapping;
  }
}