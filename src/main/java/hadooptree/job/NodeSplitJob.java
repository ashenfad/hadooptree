package hadooptree.job;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class NodeSplitJob {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String[] valueTokens = value.toString().split(",");

      String nodeString = valueTokens[0];

      StringBuilder builder = new StringBuilder();
      for (int i = 1; i < valueTokens.length; i++) {
        builder.append(valueTokens[i]);
        builder.append(",");
      }
      String newValueString = builder.toString();
      newValueString = newValueString.substring(0, newValueString.length() - 1);

      Text newKey = new Text(nodeString);
      Text newValue = new Text(newValueString);

      context.write(newKey, newValue);
    }
  }

  public static class Reduce
          extends Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      double maxInformationGain = -Double.MAX_VALUE;
      Text bestSplit = null;

      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        Text text = iter.next();
        String tokens[] = text.toString().split(",");
        double informationGain = Double.valueOf(tokens[2]);
        if (informationGain > maxInformationGain || bestSplit == null) {
          maxInformationGain = informationGain;
          bestSplit = new Text(text);
        }
      }

      Text newValue = new Text(key.toString() + "," + bestSplit.toString());
      context.write(NullWritable.get(), newValue);
    }
  }
}
