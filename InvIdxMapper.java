import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvIdxMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  private Text word = new Text();

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
