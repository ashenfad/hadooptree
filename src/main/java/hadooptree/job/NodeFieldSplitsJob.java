package hadooptree.job;

import hadooptree.Utils;
import hadooptree.tree.Field;
import hadooptree.tree.Tree;
import hadooptree.tree.Node;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class NodeFieldSplitsJob {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

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

      if (node.isLeaf() || node.getTotalCount() < Utils.DEFAULT_SUBTREE_FLOOR) {
        return;
      }

      int leafId = node.getId();
      String objectiveValueString = instance.get(tree.getObjectiveFieldIndex()).toString();

      for (int fieldId = 0; fieldId < tree.getFields().size(); fieldId++) {
        if (fieldId == tree.getObjectiveFieldIndex()) {
          continue;
        }

        String keyString = String.valueOf(leafId) + "," + String.valueOf(fieldId);

        Field field = tree.getFields().get(fieldId);
        if (!field.isCategorical()) {
          double[] range = node.getRange(node, field);
          keyString += "," + range[0] + "," + range[1];
        }

        String valueString = instance.get(fieldId).toString() + "," + objectiveValueString;

        context.write(new Text(keyString), new Text(valueString));
      }

    }
  }

  public static class Reduce
          extends Reducer<Text, Text, NullWritable, Text> {

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
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      String[] keyTokens = key.toString().split(",");
      int fieldId = Integer.valueOf(keyTokens[1]);
      Field field = tree.getFields().get(fieldId);
      boolean isCategorical = field.isCategorical();

      String result;
      Text newText;
      if (isCategorical) {
        result = reduceForCategorical(field, values);
        newText = key;
      } else {
        double[] rangeValues = new double[2];
        int leafId = Integer.valueOf(keyTokens[0]);
        rangeValues[0] = Double.valueOf(keyTokens[2]);
        rangeValues[1] = Double.valueOf(keyTokens[3]);
        result = reduceForNumeric(rangeValues, values);
        newText = new Text(String.valueOf(leafId) + "," + String.valueOf(fieldId));
      }

      context.write(NullWritable.get(), new Text(newText.toString() + "," + result));

    }

    private String reduceForCategorical(Field field, Iterable<Text> values) {

      int objectiveCategoryCount = tree.getObjectiveField().getCategorySet().size();

      ArrayList<String> objectiveCategories = new ArrayList<String>(tree.getObjectiveField().getCategorySet());
      HashMap<String, Integer> objectiveCategoryIdMap = tree.createObjectiveCategoryIdMap();

      Long[] originalCounts = new Long[objectiveCategoryCount];
      Arrays.fill(originalCounts, 0l);

      HashMap<String, Long[]> splitCounts = new HashMap<String, Long[]>();
      for (String split : field.getCategorySet()) {
        Long[] countArray = new Long[objectiveCategoryCount];
        Arrays.fill(countArray, 0l);
        splitCounts.put(split, countArray);
      }

      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        Text textValue = iter.next();
        String[] tokens = textValue.toString().split(",");
        String fieldValue = tokens[0];
        String targetCategory = tokens[1];

        int categoryId = objectiveCategoryIdMap.get(targetCategory);
        Long[] counts = splitCounts.get(fieldValue);
        counts[categoryId]++;
        originalCounts[categoryId]++;
      }

      double maxInformationGain = -Double.MAX_VALUE;
      String bestSplitValue = null;
      Long[] bestEqualToCounts = null;
      Long[] bestNotEqualToCounts = null;

      for (Entry<String, Long[]> candidateSplitEntry : splitCounts.entrySet()) {
        String candidateSplit = candidateSplitEntry.getKey();

        Long[] equalToCounts = candidateSplitEntry.getValue();
        Long[] notEqualToCounts = Utils.subtractCounts(originalCounts.clone(), equalToCounts);

        double informationGain = Utils.findInformationGain(originalCounts, equalToCounts, notEqualToCounts);
        long equalToInstanceCount = Utils.sumCounts(equalToCounts);
        long notEqualToInstanceCount = Utils.sumCounts(notEqualToCounts);

        if (informationGain > maxInformationGain
                && equalToInstanceCount > Utils.DEFAULT_SPLIT_FLOOR
                && notEqualToInstanceCount > Utils.DEFAULT_SPLIT_FLOOR) {
          maxInformationGain = informationGain;
          bestSplitValue = candidateSplit;
          bestEqualToCounts = equalToCounts.clone();
          bestNotEqualToCounts = notEqualToCounts.clone();
        }
      }

      String result = getDefaultReduceResult(objectiveCategories);

      if (maxInformationGain != -Double.MAX_VALUE) {
        StringBuilder builder = new StringBuilder();

        builder.append(String.valueOf(bestSplitValue));
        builder.append(",");
        builder.append(String.valueOf(maxInformationGain));
        builder.append(",");
        builder.append(Utils.printCounts(objectiveCategories, bestEqualToCounts));
        builder.append(",");
        builder.append(Utils.printCounts(objectiveCategories, bestNotEqualToCounts));
        result = builder.toString();
      }
      return result;
    }

    private String reduceForNumeric(double[] rangeValues, Iterable<Text> values) {

//      double range = field.getMaxValue() - field.getMinValue();
      double range = rangeValues[1] - rangeValues[0];
      int splitCount = Utils.DEFAULT_NUMERIC_SPLITS;
      int bucketCount = splitCount + 1;
      double bucketSize = range / (double) bucketCount;

      int objectiveCategoryCount = tree.getObjectiveField().getCategorySet().size();

      ArrayList<String> objectiveCategories = new ArrayList<String>(tree.getObjectiveField().getCategorySet());
      HashMap<String, Integer> objectiveCategoryIdMap = tree.createObjectiveCategoryIdMap();

      Long[] originalCounts = new Long[objectiveCategoryCount];
      Arrays.fill(originalCounts, 0l);

      TreeMap<Double, Long[]> bucketCounts = new TreeMap<Double, Long[]>();
      for (int i = 1; i <= bucketCount; i++) {
        double bucketCeiling = rangeValues[0] + (i * bucketSize);
        Long[] countArray = new Long[objectiveCategoryCount];
        Arrays.fill(countArray, 0l);
        bucketCounts.put(bucketCeiling, countArray);
      }

      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        Text textValue = iter.next();
        String[] tokens = textValue.toString().split(",");
        double fieldValue = Double.valueOf(tokens[0]);
        String targetCategory = tokens[1];

        int categoryId = objectiveCategoryIdMap.get(targetCategory);
        Entry<Double, Long[]> entry = bucketCounts.ceilingEntry(fieldValue);
        if (entry == null) {
        } else {
          Long[] counts = entry.getValue();
          counts[categoryId]++;
          originalCounts[categoryId]++;
        }

      }

      long totalInstanceCount = Utils.sumCounts(originalCounts);

      long lessThanInstanceCount = 0;
      Long[] lessThanCounts = new Long[objectiveCategoryCount];
      Arrays.fill(lessThanCounts, 0l);

      long greaterThanInstanceCount = totalInstanceCount;
      Long[] greaterThanCounts = new Long[objectiveCategoryCount];
      Arrays.fill(greaterThanCounts, 0l);
      Utils.addCounts(greaterThanCounts, originalCounts);

      double maxInformationGain = -Double.MAX_VALUE;
      double bestSplitValue = -Double.MAX_VALUE;
      Long[] bestLessThanCounts = null;
      Long[] bestGreaterThanCounts = null;

      for (Entry<Double, Long[]> candidateSplitEntry : bucketCounts.entrySet()) {
        double candidateSplit = candidateSplitEntry.getKey();
        Long[] counts = candidateSplitEntry.getValue();

        lessThanCounts = Utils.addCounts(lessThanCounts, counts);
        greaterThanCounts = Utils.subtractCounts(greaterThanCounts, counts);

        double informationGain = Utils.findInformationGain(originalCounts, lessThanCounts, greaterThanCounts);

        long instanceCount = Utils.sumCounts(counts);
        greaterThanInstanceCount -= instanceCount;
        lessThanInstanceCount += instanceCount;

        if (informationGain > maxInformationGain
                && lessThanInstanceCount > Utils.DEFAULT_SPLIT_FLOOR
                && greaterThanInstanceCount > Utils.DEFAULT_SPLIT_FLOOR) {
          maxInformationGain = informationGain;
          bestSplitValue = candidateSplit;
          bestLessThanCounts = lessThanCounts.clone();
          bestGreaterThanCounts = greaterThanCounts.clone();
        }
      }

      String result = getDefaultReduceResult(objectiveCategories);

      if (maxInformationGain != -Double.MAX_VALUE) {
        StringBuilder builder = new StringBuilder();

        builder.append(String.valueOf(bestSplitValue));
        builder.append(",");
        builder.append(String.valueOf(maxInformationGain));
        builder.append(",");
        builder.append(Utils.printCounts(objectiveCategories, bestLessThanCounts));
        builder.append(",");
        builder.append(Utils.printCounts(objectiveCategories, bestGreaterThanCounts));
        result = builder.toString();
      }
      return result;
    }

    private static String getDefaultReduceResult(ArrayList<String> categories) {
      Long[] counts = new Long[categories.size()];
      Arrays.fill(counts, 0l);

      StringBuilder builder = new StringBuilder();
      builder.append(String.valueOf(-Double.MAX_VALUE));
      builder.append(",");
      builder.append(String.valueOf(-Double.MAX_VALUE));
      builder.append(",");
      builder.append(Utils.printCounts(categories, counts));
      builder.append(",");
      builder.append(Utils.printCounts(categories, counts));
      return builder.toString();
    }
  }
}
