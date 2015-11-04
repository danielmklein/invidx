import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReduce extends Reducer<Text, IntWritable, Text, Text>
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
    System.out.println("REDUCER: term " + term + " is found in the following docs: ");

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
