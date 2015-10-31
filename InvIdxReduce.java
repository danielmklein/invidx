import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

  /*
  / reducer expects term key with list of docno's.
  / create map of docno:tf pairs (let's call it metrics)
  / for docno in docnos:
  /    if docno already in metrics:
  /        metrics.put(docno, metrics.get(docno)+1)
  /    else:
  /        metrics.put(docno, 1)
  /
  / then, we need to build a result_string of the format:
  / ": 4 : (1, 1), (2, 1), (3, 1), (4, 1)"
  /
  / : df : [(metrics.get(docno)) for docno in metrics.keyset().sorted()]
  / finally, emit (term, result_string)
  */

  @Override
  public void reduce(Text term, Iterable<IntWritable> docnos, Context context)
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

    List<Integer> sortedDocNos = new List<Integer>();
    sortedDocNos.addAll(termFreqs.keySet());
    Collections.sort(sortedDocNos);

    StringBuilder sb = new StringBuilder();
    sb.append(": ").append(df).append(" : ");

    Integer docNo;
    for (int i = 0; i < sortedDocNos.size(); i++)
    {
      Integer docNo = sortedDocNos.get(i);
      sb.append("(").append(docNo).append(",").append(termFreqs.get(docNo)).append(")");
      if (i + 1 < sortedDocNos.size())
      {
        sb.append(", ");
      }
    }

    System.out.println("REDUCER: final output line for term " + term + " is " + sb.toString());

    context.write(term, new Text(sb.toString()));
  }
}
