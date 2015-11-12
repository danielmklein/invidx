/*
/ building and running:
/ $ HADOOP_CLASSPATH="$(hadoop classpath)"
/ $ mkdir invidx_classes
/ $ javac -classpath ${HADOOP_CLASSPATH} -d invidx_classes *.java
/ $ jar -cvf /home/hadoop/InvIdx.jar -C invidx_classes/ .
/
/ $ hadoop jar ./InvIdx.jar InvIdxDriver /merchants.txt <term>
**/

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
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
        // TODO: this inPath might get hardcoded, in which case I will need to
        // change termToQuery to look at args[0]
        String inPath = "input.txt";// args[0]; // first arg is the path of the input file.
        String termToQuery = "";
        if (args.length > 0)
        {
          termToQuery = args[0].trim(); // second arg is the term for which to print result.
        }

        String hdfsInPath = "/input.txt";
        writeInputToHdfs(inPath, hdfsInPath);

        System.out.println("DRIVER: Executing inverted index MapReduce job, using " + inPath + " for input.");
        String outputPath = "/invidx/output";
        String outputFile = "/invidx/output/part-r-00000";

        // blow away the leftover output directory and its contents.
        FileSystem fs = FileSystem.get(createConfig());
        fs.delete((new Path(outputPath)), true);

        boolean successful = calculate(hdfsInPath, outputPath);
        if (!successful)
        {
          System.out.println("DRIVER: something went wrong with the MapReduce job.");
          return 1;
        }

        String resultString = "";
        if (termToQuery.equals(""))
        {
          resultString = getFullOutput(outputFile);
        } else
        {
          resultString = searchOutputFile(outputFile, termToQuery);
        }
        System.out.println("*** RESULTS ***");
        System.out.println(resultString);

        return 0;
    }

    private Configuration createConfig()
    {
      Configuration config = new Configuration();
      config.addResource(new Path("/HADOOP_HOME/conf/core-site.xml"));
      config.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
      return config;
    }

    private void writeInputToHdfs(String localInPath, String hdfsOutPath)
    {
      Configuration config = createConfig();
      BufferedReader br = null;
      BufferedWriter bw = null;
      String line;
      System.out.println("DRIVER: writing local input file to HDFS.");

      try
      {
        br = new BufferedReader(new FileReader(localInPath));
        FileSystem fs = FileSystem.get(config);
        Path outPath = new Path(hdfsOutPath);
        bw = new BufferedWriter(new OutputStreamWriter(fs.create(outPath, true)));
        line = br.readLine();
        while (line != null)
        {
          System.out.println("DRIVER: writing line " + line + " to input file on HDFS.");
          bw.write(line);
          bw.newLine();
          line = br.readLine();
        }
      } catch (Exception e) {}
      finally
      {
        try
        {
          if (br != null) br.close();
        } catch (IOException e) {}
        try
        {
          if (bw != null) bw.close();
        } catch (IOException e) {}
      }
      System.out.println("DRIVER: finished writing local input file to HDFS.");

    }

    /*
    /  Read output file and return a string containing its entire contents.
    */
    private String getFullOutput(String outputFile) throws IOException
    {
      FileSystem fs = FileSystem.get(createConfig());
      Path path = new Path(outputFile);
      BufferedReader br = null;
      StringBuilder fileSb = new StringBuilder();
      System.out.println("DRIVER: no term specified, so displaying entire output file.");
      try
      {
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null)
        {
          fileSb.append(line).append("\n");
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

      return fileSb.toString();
    }

    /*
    / Given a MapReduce output file path and a term, find and return the line in
    / the file corresponding to the term, or return an empty string if term is not found.
    */
    private String searchOutputFile(String outputFile, String term) throws IOException
    {
      FileSystem fs = FileSystem.get(createConfig());
      Path path = new Path(outputFile);
      BufferedReader br = null;
      String resultString = "";
      System.out.println("DRIVER: searching output file for term: " + term);
      try
      {
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        while (line != null)
        {
          if (line.startsWith(term + " ") || line.startsWith(term + "\t"))
          {
            resultString = line;
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

      return resultString;
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
