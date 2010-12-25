package hadooptree;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.jdom.output.XMLOutputter;

public class DefineFieldsJob {

  public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");

      int fieldId = 0;
      for (String token : tokens) {
        String trimmedToken = token.trim();
        Text outputValue = null;
        if (!trimmedToken.isEmpty()) {
          outputValue = new Text(trimmedToken);
        }

        IntWritable outputKey = new IntWritable(fieldId);
        if (outputValue != null) {
          context.write(outputKey, outputValue);
        }
        fieldId++;
      }
    }
  }

  public static class Reduce
          extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    // TODO - Add better exception handling

      try {
        Field fieldDefinition = new Field(key.get());

        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
          Text text = iter.next();
          try {
            double number = Double.valueOf(text.toString());
            fieldDefinition.addNumericValue(number);
          } catch (NumberFormatException e) {
            fieldDefinition.addCategoricalValue(text.toString());
          }
        }

        XMLOutputter outputter = new XMLOutputter();
        String xmlString = outputter.outputString(fieldDefinition.toElement());
        Text outputValue = new Text(xmlString);
        context.write(NullWritable.get(), outputValue);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
