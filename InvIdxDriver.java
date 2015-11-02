/*
/ building and running:
/ $ HADOOP_CLASSPATH="$(hadoop classpath)"
/ $ mkdir invidx_classes
/ $ javac -classpath ${HADOOP_CLASSPATH} -d invidx_classes *.java
/ $ jar -cvf /home/hadoop/InvIdx.jar -C invidx_classes/ .
/
/ $ hadoop jar ./InvIdx.jar InvIdxDriver /merchants.txt us
**/

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InvIdxDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new InvIdxDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        String inPath = args[0];
        String termToQuery = args[1].trim();
        boolean isCompleted;
        System.out.println("DRIVER: Executing inverted index MapReduce job, using " + inPath + " for input.");
        String outputPath = "/invidx/output";
        String outputFile = "/invidx/output/part-r-00000";

        FileSystem fs = FileSystem.get(createConfig());
        fs.delete((new Path(outputPath)), true); // blow away the leftover output directory and its contents.

        isCompleted = calculate(inPath, outputPath);
        if (!isCompleted)
        {
          System.out.println("DRIVER: something went wrong with the MapReduce job.");
          return 1;
        }

        String outString = searchOutputFile(outputFile, termToQuery);
        System.out.println("DRIVER: result is: " + outString);
        System.out.println(outString);

        return 0;
    }

    private Configuration createConfig()
    {
      Configuration config = new Configuration();
      config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
      config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
      return config;
    }

    private String searchOutputFile(String outputFile, String term) throws IOException
    {
      FileSystem fs = FileSystem.get(createConfig());
      Path path = new Path(outputFile);
      BufferedReader br = null;
      String outString = "";
      System.out.println("DRIVER: searching output file for term: " + term);
      try
      {
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null)
        {
          if (line.startsWith(term + " ") || line.startsWith(term + "\t"))
          {
            outString = line;
            break;
          }
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

      return outString;
    }

    /*
    // This method actually sets up and runs the mapreduce job.
    */
    private boolean calculate(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = createConfig();
        Job invIdx = Job.getInstance(conf, "InvIdx");
        invIdx.setJarByClass(InvIdxDriver.class);
        invIdx.setInputFormatClass(NLineInputFormat.class);
        invIdx.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
        invIdx.setMapOutputKeyClass(Text.class);
        invIdx.setMapOutputValueClass(IntWritable.class);
        invIdx.setOutputKeyClass(Text.class);
        invIdx.setOutputValueClass(Text.class);
        invIdx.setOutputFormatClass(TextOutputFormat.class);
        invIdx.setMapperClass(InvIdxMapper.class);
        invIdx.setReducerClass(InvIdxReduce.class);
        FileInputFormat.setInputPaths(invIdx, new Path(inputPath));
        FileOutputFormat.setOutputPath(invIdx, new Path(outputPath));

        return invIdx.waitForCompletion(false);
    }
}
