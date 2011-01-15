package hadooptree.job;

import hadooptree.Utils;
import hadooptree.tree.Node;
import hadooptree.tree.Tree;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FilterInstancesJob {

  public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

    private Tree tree;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      Configuration conf = context.getConfiguration();

      try {
        this.tree = Utils.loadTree(conf);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

      String instanceString = value.toString();
      instanceString = instanceString.trim();
      if (instanceString.isEmpty()) {
        return;
      }

      ArrayList<Object> instance;
      try {
        instance = Utils.convertInstanceStringToArrayList(instanceString, tree.getFields());
      } catch (Exception e) {
        throw new IOException(e);
      }

      Node node = tree.evalToNode(instance);

      if (!node.isLeaf()) {
        context.write(NullWritable.get(), value);
      }
    }
  }

}
