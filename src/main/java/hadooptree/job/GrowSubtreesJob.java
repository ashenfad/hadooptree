package hadooptree.job;

import hadooptree.SplitResults;
import hadooptree.Utils;
import hadooptree.tree.Field;
import hadooptree.tree.Node;
import hadooptree.tree.Split;
import hadooptree.tree.Tree;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

public class GrowSubtreesJob {

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

      if (!node.isLeaf() && node.getTotalCount() < Utils.DEFAULT_SUBTREE_FLOOR) {
        int leafId = node.getId();
        context.write(new Text(String.valueOf(leafId)), value);
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

      ArrayList<ArrayList<Object>> instances = new ArrayList<ArrayList<Object>>();

      // TODO - Add better exception handling
      try {
        Iterator<Text> iter = values.iterator();
        while (iter.hasNext()) {
          Text value = iter.next();
          String instanceString = value.toString();
          instanceString = instanceString.trim();
          if (instanceString.isEmpty()) {
            continue;
          }
          ArrayList<Object> instance = Utils.convertInstanceStringToArrayList(instanceString, tree.getFields());
          instances.add(instance);
        }

      } catch (Exception e) {
        throw new IOException(e);
      }

      Node originalNode = tree.evalToNode(instances.get(0));

      TreeMap<String, Long> objectiveCategoryCountMap = originalNode.getObjectiveCategoryCountMap();
      HashMap<String, Integer> objectiveCategoryIdMap = tree.createObjectiveCategoryIdMap();

      Long[] counts = new Long[objectiveCategoryIdMap.size()];
      for (Entry<String, Integer> entry : objectiveCategoryIdMap.entrySet()) {
        String category = entry.getKey();
        int index = entry.getValue();
        long count = objectiveCategoryCountMap.get(category);
        counts[index] = count;
      }

      Node newRoot = createNode(null, instances, objectiveCategoryIdMap, counts);
      newRoot.setId(originalNode.getId());

      Element element = newRoot.toElement("subtreeRoot");
      XMLOutputter outputter = new XMLOutputter();
      Text value = new Text(outputter.outputString(element));

      context.write(NullWritable.get(), value);
    }

    private Node createNode(Node parent, ArrayList<ArrayList<Object>> instances,
            HashMap<String, Integer> objectiveCategoryIdMap,
            Long[] originalCounts) {

      SplitResults bestSplitResults = null;

      for (int fieldIndex = 0; fieldIndex < tree.getFields().size(); fieldIndex++) {
        if (fieldIndex == tree.getObjectiveFieldIndex()) {
          continue;
        }

        Field field = tree.getFields().get(fieldIndex);

        SplitResults splitResults = null;
        if (field.isCategorical()) {
          splitResults = findBestCategoricalSplit(field, instances, objectiveCategoryIdMap, originalCounts);
        } else {
          splitResults = findBestNumericSplit(field, instances, objectiveCategoryIdMap, originalCounts);
        }

        if (splitResults != null && (bestSplitResults == null
                || splitResults.getInformationGain() > bestSplitResults.getInformationGain())) {
          bestSplitResults = splitResults;
        }
      }

      TreeMap<String, Long> objectiveCategoryCountMap = new TreeMap<String, Long>();
      for (Entry<String, Integer> entry : objectiveCategoryIdMap.entrySet()) {
        String category = entry.getKey();
        int index = entry.getValue();
        long count = originalCounts[index];
        objectiveCategoryCountMap.put(category, count);
      }

      Node node = new Node(-1, parent, objectiveCategoryCountMap);
      if (bestSplitResults == null) {
        node.setIsLeaf(true);
      } else {
        ArrayList<ArrayList<Object>> trueInstances = new ArrayList<ArrayList<Object>>();
        ArrayList<ArrayList<Object>> falseInstances = new ArrayList<ArrayList<Object>>();

        for (ArrayList<Object> instance : instances) {
          if (bestSplitResults.getSplit().eval(instance)) {
            trueInstances.add(instance);
          } else {
            falseInstances.add(instance);
          }
        }

        Long[] trueCounts = bestSplitResults.getTrueCounts();
        Node trueChild = createNode(node, trueInstances, objectiveCategoryIdMap, trueCounts);

        Long[] falseCounts = bestSplitResults.getFalseCounts();
        Node falseChild = createNode(node, falseInstances, objectiveCategoryIdMap, falseCounts);

        node.addSplit(bestSplitResults.getSplit(), trueChild, falseChild);
      }

      return node;
    }

    private SplitResults findBestCategoricalSplit(Field field, ArrayList<ArrayList<Object>> instances, HashMap<String, Integer> objectiveCategoryIdMap, Long[] originalCounts) {

      String bestSplitCategory = null;
      double maxInformationGain = -Double.MAX_VALUE;

      Long[] bestTrueCounts = null;
      Long[] bestFalseCounts = null;

      for (String category : field.getCategorySet()) {
        Long[] trueCounts = new Long[originalCounts.length];
        Arrays.fill(trueCounts, 0l);

        Long[] falseCounts = new Long[originalCounts.length];
        Arrays.fill(falseCounts, 0l);

        int objectiveCategoryId = objectiveCategoryIdMap.get(category);

        for (ArrayList<Object> instance : instances) {
          if (category.equals(instance.get(field.getIndex()))) {
            trueCounts[objectiveCategoryId]++;
          } else {
            falseCounts[objectiveCategoryId]++;
          }
        }

        double informationGain = Utils.findInformationGain(originalCounts, trueCounts, falseCounts);

        long trueInstanceCount = Utils.sumCounts(trueCounts);
        long falseInstanceCount = Utils.sumCounts(falseCounts);

        if (informationGain > maxInformationGain
                && trueInstanceCount > Utils.DEFAULT_SPLIT_FLOOR
                && falseInstanceCount > Utils.DEFAULT_SPLIT_FLOOR) {
          maxInformationGain = informationGain;
          bestSplitCategory = category;
          bestTrueCounts = trueCounts;
          bestFalseCounts = falseCounts;
        }
      }

      SplitResults bestSplitResults = null;
      if (bestSplitCategory != null) {
        Split bestSplit = new Split(field.getIndex(), bestSplitCategory);
        bestSplitResults = new SplitResults(bestSplit, maxInformationGain, bestTrueCounts, bestFalseCounts);
      }

      return bestSplitResults;
    }

    private SplitResults findBestNumericSplit(Field field, ArrayList<ArrayList<Object>> instances, HashMap<String, Integer> objectiveCategoryIdMap, Long[] originalCounts) {

      double bestSplitPoint = -Double.MAX_VALUE;
      double maxInformationGain = -Double.MAX_VALUE;

      Long[] bestTrueCounts = null;
      Long[] bestFalseCounts = null;

      Long[] greaterThanCounts = originalCounts.clone();
      Long[] lessThanCounts = new Long[originalCounts.length];
      Arrays.fill(lessThanCounts, 0l);

      int fieldId = field.getIndex();
      int objectiveFieldId = tree.getObjectiveFieldIndex();

      ArrayList<NumericCategoricalPair> pairInstances = new ArrayList<NumericCategoricalPair>();
      for (ArrayList<Object> instance : instances) {
        Double fieldValue = (Double) instance.get(fieldId);
        String objectiveFieldValue = (String) instance.get(objectiveFieldId);
        NumericCategoricalPair pairInstance = new NumericCategoricalPair(fieldValue, objectiveFieldValue);
        pairInstances.add(pairInstance);
      }

      Collections.sort(pairInstances);

      NumericCategoricalPair previousPairInstance = null;

      for (NumericCategoricalPair pairInstance : pairInstances) {
        if (previousPairInstance == null) {
          previousPairInstance = pairInstance;
          continue;
        }

        String previousObjectiveCategory = previousPairInstance.getCategoricalValue();
        int previousObjectiveCategoryId = objectiveCategoryIdMap.get(previousObjectiveCategory);

        greaterThanCounts[previousObjectiveCategoryId]--;
        lessThanCounts[previousObjectiveCategoryId]++;

        double previousFieldValue = previousPairInstance.getNumericValue();
        double currentFieldValue = pairInstance.getNumericValue();

        if (previousFieldValue == currentFieldValue) {
          previousPairInstance = pairInstance;
          continue;
        }

        double informationGain = Utils.findInformationGain(originalCounts, greaterThanCounts, lessThanCounts);

        long lessThanInstanceCount = Utils.sumCounts(lessThanCounts);
        long greaterThanInstanceCount = Utils.sumCounts(greaterThanCounts);

        if (informationGain > maxInformationGain
                && greaterThanInstanceCount > Utils.DEFAULT_SPLIT_FLOOR
                && lessThanInstanceCount > Utils.DEFAULT_SPLIT_FLOOR) {
          maxInformationGain = informationGain;
          bestSplitPoint = (previousFieldValue + currentFieldValue) / 2d;
          bestTrueCounts = lessThanCounts.clone();
          bestFalseCounts = greaterThanCounts.clone();
        }

        previousPairInstance = pairInstance;
      }

      SplitResults bestSplitResults = null;
      if (maxInformationGain != -Double.MAX_VALUE) {
        Split bestSplit = new Split(field.getIndex(), bestSplitPoint);
        bestSplitResults = new SplitResults(bestSplit, maxInformationGain, bestTrueCounts, bestFalseCounts);
      }

      return bestSplitResults;
    }

    private class NumericCategoricalPair implements Comparable<NumericCategoricalPair> {

      private final double numericValue;
      private final String categoricalValue;

      public NumericCategoricalPair(double numericValue, String categoricalValue) {
        this.numericValue = numericValue;
        this.categoricalValue = categoricalValue;
      }

      public String getCategoricalValue() {
        return categoricalValue;
      }

      public double getNumericValue() {
        return numericValue;
      }

      @Override
      public int compareTo(NumericCategoricalPair t) {
        int result = Double.compare(numericValue, t.getNumericValue());
        if (result == 0) {
          result = categoricalValue.compareTo(t.getCategoricalValue());
        }
        return result;
      }
    }
  }
}
