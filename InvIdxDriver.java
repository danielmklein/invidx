/*
/ building and running:
/ $ HADOOP_CLASSPATH="$(hadoop classpath)"
/ $ mkdir pagerank_classes
/ $ javac -classpath ${HADOOP_CLASSPATH} -d invidx_classes *.java
/ $ jar -cvf /home/hadoop/InvIdx.jar -C invidx_classes/ .
/
/ $ hadoop jar ./InvIdx.jar InvIdxDriver /pagerank/graph.txt
**/

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;
import java.text.NumberFormat;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
/  TODO: rewrite mapper, reducer, and driver
/  I suspect that most or all of the file rewriting will not be necessary for this.
/
/  BUT I should rewrite the driver so that user can specify term as command line arg.
/  So the args should be <inputfilepath> <term to show indx>
/  
/  For simplicity, Driver should just blow away results from previous invidx MR job.
*/
public class InvIdxDriver extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");
    private Integer numNodes;
    private Integer numEdges;
    private Integer numIterations;
    private Map<String, List<String>> outlinks;
    private Map<String, Float> pageranks;

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new PageRankDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputFilePath = args[0];
        boolean isCompleted;
        String lastResultPath = null;
        System.out.println("DRIVER: Constructing initial input file...");
        // prepareInitialInputFile() takes graph.txt (or other input file) and rewrites it in
        // the format that my mapper is expecting.
        prepareInitialInputFile(inputFilePath);
        System.out.println("DRIVER: Initial input file complete.");

        for (int curRun = 1; curRun <= this.numIterations; curRun++) {
            System.out.println("DRIVER: Executing iteration " + curRun + " of " + this.numIterations);
            String inPath = "/pagerank/input/iter" + nf.format(curRun);
            lastResultPath = "/pagerank/input/iter" + nf.format(curRun + 1);
            isCompleted = calculate(inPath, lastResultPath);
            if (!isCompleted)
            {
              System.out.println("DRIVER: something went wrong with the MapReduce job.");
              return 1;
            }
            if (curRun < this.numIterations)
            {
              // transformOutputFile() takes the MR job output and rewrites it
              // to the format that my mapper is expecting.
              transformOutputFile(lastResultPath);
            }
        }

        return 0;
    }

    private Configuration createConfig()
    {
      Configuration config = new Configuration();
      config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
      config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
      return config;
    }

    /*
    // Read the initial input file, save the graph structure, and rewrite it
    // into the first input file for map reduce.
    */
    private void prepareInitialInputFile(String initInputPath) throws Exception, IOException
    {
      Configuration config = createConfig();
      String fromNodeId;
      String toNodeId;
      this.outlinks = new HashMap<String, List<String>>();
      this.pageranks = new HashMap<String, Float>();
      FileSystem fs = FileSystem.get(config);
      Path path = new Path(initInputPath);
      BufferedReader br = null;
      try
      {
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        this.numNodes = Integer.parseInt(line.split("\\s+")[0]);
        System.out.println("DRIVER: Number of nodes in graph: " + this.numNodes);
        this.numEdges = Integer.parseInt(line.split("\\s+")[1]);
        System.out.println("DRIVER: Number of edges in graph: " + this.numEdges);
        line = br.readLine();
        this.numIterations = Integer.parseInt(line.trim());
        System.out.println("DRIVER: Number of iterations to run: " + numIterations);
        // initValue is the seed pagerank that each node gets
        Float initValue = (new Float(1)) / (new Float(this.numNodes));
        System.out.println("DRIVER: Constructing graph outlink table...");
        line = br.readLine();
        while (line != null)
        {
          // parse out the outlink and add it to the proper node
          fromNodeId = line.split("\\s+")[0].trim();
          toNodeId = line.split("\\s+")[1].trim();
          if (this.outlinks.get(fromNodeId) == null)
          {
            // if fromNode not in outlinks table already, add it
            // and put an initial pagerank in the pageranks table for it
            System.out.println("DRIVER: Adding node " + fromNodeId + " to table.");
            this.outlinks.put(fromNodeId, new ArrayList<String>());
            this.pageranks.put(fromNodeId, initValue);
          }
          System.out.println("DRIVER: Adding link " + fromNodeId + " -> " + toNodeId);
          this.outlinks.get(fromNodeId).add(toNodeId);
          line = br.readLine();
        }
      } catch (Exception e)
      {} finally
      {
        try
        {
          if (br != null) br.close();
        } catch (IOException e) {}
      }
      System.out.println("DRIVER: Graph outlink table complete.");
      System.out.println("DRIVER: Writing initial input file for MapReduce.");
      // rewriteOutput() performs the actual writing of the graph structure
      // with pagerank values to the file.
      rewriteOutput("/pagerank/input/iter01");
    }

    /*
    // Read in the new pagerank value for each node from the output file,
    // then store each in the pagerank table (this.pageranks).
    // Then use that table and the outlinks table to rewrite a new input
    // file for the next iteration.
    */
    private void transformOutputFile(String outputDir)
    {
        System.out.println("DRIVER: rewriting output from previous iteration to include graph structure.");
        Configuration config = createConfig();
        BufferedReader br = null;
        String outputPath = outputDir + "/part-r-00000";
        String nid;
        Float newValue;
        try
        {
          FileSystem fs = FileSystem.get(config);
          Path path = new Path(outputPath);
          br = new BufferedReader(new InputStreamReader(fs.open(path)));
          String line = br.readLine();
          while (line != null)
          {
            // get the new pagerank value for each node and save to table
            nid = line.split("\\s+")[0].trim();
            newValue = Float.parseFloat(line.split("\\s+")[1]);
            System.out.println("DRIVER: new pagerank value for node " + nid + " is " + newValue);
            this.pageranks.put(nid, newValue);
            line = br.readLine();
          }
        } catch (Exception e)
        {} finally
        {
          try
          {
            if (br != null) br.close();
          } catch (IOException e) {}
        }
        rewriteOutput(outputPath);
    }

    private void rewriteOutput(String outputPath)
    {
      Configuration config = createConfig();
      // write everything from the outlinks table and pageranks tables
      // back to the same output file from which we read the pageranks.
      BufferedWriter bw = null;
      try
      {
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(outputPath);
        bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));
        String line;
        StringBuilder sb;

        for (String nodeId : this.outlinks.keySet())
        {
          sb = new StringBuilder();
          sb.append(nodeId).append(" ");
          sb.append(this.pageranks.get(nodeId)).append(" ");
          for (String outlinkId : this.outlinks.get(nodeId))
          {
            sb.append(outlinkId).append(" ");
          }
          System.out.println("DRIVER: Writing line: " + sb.toString() + " to file " + outputPath);
          sb.append("\n");
          bw.write(sb.toString());
        }
      } catch (Exception e)
      {} finally
      {
        try
        {
          if (bw != null) bw.close();
        } catch (IOException e) {}
      }
      System.out.println("DRIVER: finished rewriting previous iteration's output.");
    }

    /*
    // This method actually sets up and runs the mapreduce job.
    */
    private boolean calculate(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = createConfig();
        Job pageRank = Job.getInstance(conf, "PageRank");
        pageRank.setJarByClass(PageRankDriver.class);
        pageRank.setInputFormatClass(NLineInputFormat.class);
        pageRank.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
        pageRank.setOutputKeyClass(Text.class);
        pageRank.setOutputValueClass(FloatWritable.class);
        pageRank.setOutputFormatClass(TextOutputFormat.class);
        pageRank.setMapperClass(PageRankMapper.class);
        pageRank.setReducerClass(PageRankReduce.class);
        FileInputFormat.setInputPaths(pageRank, new Path(inputPath));
        FileOutputFormat.setOutputPath(pageRank, new Path(outputPath));

        return pageRank.waitForCompletion(true);
    }
}
