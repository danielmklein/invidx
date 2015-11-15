/*
/ building and running:
/ $ HADOOP_CLASSPATH="$(hadoop classpath)"
/ $ mkdir invidx_classes
/ $ javac -classpath ${HADOOP_CLASSPATH} -d invidx_classes *.java
/ $ jar -cvf /home/hadoop/InvIdx.jar -C invidx_classes/ .
/
/ $ hadoop jar ./InvIdx.jar InvIdxDriver <term>
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
        String termToQuery = "";
        if (args.length > 0)
        {
          termToQuery = args[0].trim(); // second arg is the term for which to print result.
        }

        String localInPath = "input.txt";
        String hdfsInPath = "/input.txt";
        String outputPath = "/invidx/output";
        String outputFile = "/invidx/output/part-r-00000";

        // blow away the leftover input file and output directory.
        FileSystem fs = FileSystem.get(createConfig());
        fs.delete((new Path(outputPath)), true);
        fs.delete((new Path(hdfsInPath)), true);

        writeInputToHdfs(localInPath, hdfsInPath);
        System.out.println("DRIVER: Executing inverted index MapReduce job, using " + localInPath + " for input.");
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
        System.out.println("\n******* RESULTS *******\n");
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
          System.out.println("DRIVER: writing line '" + line + "'");
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

    private class InvIdxMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
      private Text word = new Text();

      /*
      / This mapper expects a line of the format "docno: term1 term2 ... termn",
      / extracts the docno from the line, and then for each term,
      / emits (term, docno).
      */
      @Override
      public void map(LongWritable key, Text value, Context context)
                      throws IOException, InterruptedException
      {
        String line = value.toString();
        List<String> fields = Arrays.asList(line.split("\\s+"));
        Integer docNo = Integer.parseInt(fields.get(0).replaceAll(":", ""));

        System.out.println("MAPPER: currently processing doc number " + docNo);

        for (String term : fields.subList(1, fields.size()))
        {
          System.out.println("MAPPER: emitting (term, docno) pair (" + term + "," + docNo + ")");
          word.set(term.trim());
          context.write(word, new IntWritable(docNo));
        }
      }
    }

    private class InvIdxReduce extends Reducer<Text, IntWritable, Text, Text>
    {
      /*
      / This reducer expects a key (a term for which to calculate metrics), along with
      / a list of doc numbers. It builds a map of (docno, termfreq) pairs for the
      / given term, and uses this to calculate the document frequency for the term.
      / Finally, we build an output string containing the doc freq for the term along
      / with the tf for the term for each doc in which it appears.
      */
      @Override
      public void reduce(Text term, Iterable<IntWritable> docNos, Context context)
                        throws IOException, InterruptedException
      {
        System.out.print("REDUCER: term '" + term + "' is found in the following docs: ");

        Map<Integer, Integer> termFreqs = new HashMap<Integer, Integer>();
        for (IntWritable docNo : docNos)
        {
          System.out.print(docNo + " ");
          Integer pojoDocNo = docNo.get();
          Integer oldTf = termFreqs.get(pojoDocNo);
          if (oldTf == null)
          {
            termFreqs.put(pojoDocNo, 1);
          } else
          {
            termFreqs.put(pojoDocNo, oldTf+1);
          }
        }
        System.out.print("\n");

        List<Integer> sortedDocNos = new ArrayList<Integer>();
        sortedDocNos.addAll(termFreqs.keySet());
        Collections.sort(sortedDocNos);
        StringBuilder sb = new StringBuilder();
        Integer df = sortedDocNos.size();
        sb.append(": ").append(df).append(" : ");
        Integer doc;
        for (int i = 0; i < sortedDocNos.size(); i++)
        {
          doc = sortedDocNos.get(i);
          sb.append("(").append(doc).append(",").append(termFreqs.get(doc)).append(")");
          if (i + 1 < sortedDocNos.size())
          {
            sb.append(", ");
          }
        }

        context.write(term, new Text(sb.toString()));
      }
    }
}
