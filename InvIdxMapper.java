import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvIdxMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

  private Text word = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context)
                  throws IOException, InterruptedException
  {
    String line = value.toString();
    List<String> fields = Arrays.asList(line.split("\\s+"));
    String curNid = fields.get(0);
    Float initPageRank = Float.parseFloat(fields.get(1));
    Float portion = initPageRank / (new Float(fields.size() - 2));

    System.out.println("MAPPER: node " + curNid + " has current value " + initPageRank);

    for (String outlinkId : fields.subList(2, fields.size()))
    {
      System.out.println("MAPPER: sending " + portion + " to " + outlinkId);
      word.set(outlinkId);
      context.write(word, new FloatWritable(portion));
    }
  }
}
