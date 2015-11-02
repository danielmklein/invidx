import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReduce extends Reducer<Text, IntWritable, Text, Text>
{

  @Override
  public void reduce(Text term, Iterable<IntWritable> docNos, Context context)
                    throws IOException, InterruptedException
  {
    System.out.print("REDUCER: term " + term + " is found in the following docs: ");

    Map<Integer, Integer> termFreqs = new HashMap<Integer, Integer>();

    for (IntWritable docNo : docNos)
    {
      System.out.print(docNo + " ");
      Integer pojoDocNo = docNo.get();
      // Compile the term freq for our current term for each document.
      Integer oldTf = termFreqs.get(pojoDocNo);
      if (oldTf == null)
      {
        termFreqs.put(pojoDocNo, 1);
      } else
      {
        termFreqs.put(pojoDocNo, oldTf+1);
      }
    }

    // df (document frequency) is the number of distinct docs in which current term appears.
    Integer df = termFreqs.keySet().size();

    List<Integer> sortedDocNos = new ArrayList<Integer>();
    sortedDocNos.addAll(termFreqs.keySet());
    Collections.sort(sortedDocNos);

    StringBuilder sb = new StringBuilder();
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

    System.out.println("REDUCER: final output line for term " + term + " is " + sb.toString());

    context.write(term, new Text(sb.toString()));
  }
}
