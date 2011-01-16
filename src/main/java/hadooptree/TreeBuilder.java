package hadooptree;

import hadooptree.job.NodeFieldSplitsJob;
import hadooptree.job.NodeSplitsJob;
import hadooptree.job.DefineFieldsJob;
import hadooptree.job.FilterInstancesJob;
import hadooptree.job.GrowSubtreesJob;
import hadooptree.tree.Field;
import hadooptree.tree.Tree;
import hadooptree.tree.Node;
import hadooptree.tree.Split;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;

public class TreeBuilder {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 3) {
      System.err.println("Usage: hadooptree <in> <out> objectiveFieldId");
      System.exit(2);
    }

    Path inputPath = new Path(otherArgs[0]);
    Path outputPath = new Path(otherArgs[1]);
    Path dataPath = inputPath;
    Path fieldPath = new Path(outputPath, "fields");
    Path categorySplitsPath = new Path(outputPath, "categorySplits");
    Path fieldSplitsPath = new Path(outputPath, "fieldSplits");
    Path subtreesPath = new Path(outputPath, "subtrees");
    Path filteredInstancesPath = new Path(outputPath, "filteredInstances");
    Path treePath = new Path(outputPath, "tree/tree.xml");

    int objectiveFieldId = Integer.valueOf(otherArgs[2]);

    Job defineFieldsJob = setupDefineFieldsJob(args, conf, inputPath, fieldPath);

    boolean result = defineFieldsJob.waitForCompletion(true);

    if (!result) {
      System.exit(1);
    }

    ArrayList<Field> fields = readFieldDefinitions(conf, fieldPath);
    TreeMap<String, Long> classCategoryCounts = fields.get(objectiveFieldId).getCategoryMap();

    HashMap<Integer, Node> nodeMap = new HashMap<Integer, Node>();

    Node treeRoot = new Node(nodeMap.size(), null, classCategoryCounts);
    long currentInstanceCount = treeRoot.getTotalCount();
    nodeMap.put(treeRoot.getId(), treeRoot);

    Tree tree = new Tree(treeRoot, fields, objectiveFieldId);

    FileSystem fs = FileSystem.get(conf);

//    FSDataOutputStream treeOut = fs.create(treePath);
//    treeOut.writeUTF(treeXml);
//    treeOut.close();

    writeTree(tree, fs, treePath);


    boolean grewTree = true;
    long currentLeafInstanceCount = 0;
    if (treeRoot.getTotalCount() <= Utils.DEFAULT_SUBTREE_FLOOR) {
      currentLeafInstanceCount += treeRoot.getTotalCount();
    }

    int filterIteration = 0;

    while (grewTree) {
      DistributedCache.addCacheFile(treePath.toUri(), conf);

      double ratio = (double) currentLeafInstanceCount / (double) currentInstanceCount;
      if (ratio > Utils.SUBTREE_AND_LEAF_RATIO) {
        Job growSubtreesJob = growSubtreesJob(args, conf, inputPath, subtreesPath);
        result = growSubtreesJob.waitForCompletion(true);

        if (!result) {
          System.exit(1);
        }

        ArrayList<Node> subtrees = readSubtrees(conf, subtreesPath, tree.getObjectiveFieldIndex());
        fs.delete(subtreesPath, true);

        if (!subtrees.isEmpty()) {
          for (Node subtree : subtrees) {
            Node originalNode = nodeMap.get(subtree.getId());
            originalNode.merge(subtree, nodeMap);
          }

          writeTree(tree, fs, treePath);
        }

        Path filterOutputPath = new Path(filteredInstancesPath, String.valueOf(filterIteration));
        Job filteredInstancesJob = filterInstancesJob(args, conf, dataPath, filterOutputPath);
        result = filteredInstancesJob.waitForCompletion(true);

        if (!result) {
          System.exit(1);
        }

        dataPath = filterOutputPath;

        currentInstanceCount -= currentLeafInstanceCount;
        currentLeafInstanceCount = 0;
        filterIteration++;
      }

      Job categorySplitsJob = findBestCategorySplitJob(args, conf, dataPath, categorySplitsPath);
      result = categorySplitsJob.waitForCompletion(true);

      if (!result) {
        System.exit(1);
      }

      Job fieldSplitsJob = findBestFieldSplitJob(args, conf, categorySplitsPath, fieldSplitsPath);
      result = fieldSplitsJob.waitForCompletion(true);

      if (!result) {
        System.exit(1);
      }

      BuildResults results = readNewSplits(tree, nodeMap, conf, fieldSplitsPath);
      grewTree = results.isGrewTree();
      currentLeafInstanceCount += results.getLeafInstanceCount();

      writeTree(tree, fs, treePath);

//      fs.delete(treePath, true);
//      treeOut = fs.create(treePath);
//      treeOut.writeUTF(tree.toString());
//      treeOut.close();

//      System.out.println(tree.toString());

      System.out.println("CurrentInstanceCount: " + currentInstanceCount);
      System.out.println("CurrentLeafInstanceCount: " + currentLeafInstanceCount);

      fs.delete(categorySplitsPath, true);
      fs.delete(fieldSplitsPath, true);
    }

    System.exit(0);
  }

  private static void writeTree(Tree tree, FileSystem fs, Path treePath) throws IOException {
    XMLOutputter outputter = new XMLOutputter();
    String treeXml = outputter.outputString(tree.toElement());

    File treeFile = new File("tree.xml");
    treeFile.delete();
    System.out.println("Writing tree to: " + treeFile.getAbsolutePath());
    FileWriter writer = new FileWriter(treeFile);
    writer.write(treeXml);
    writer.close();
    fs.copyFromLocalFile(false, true, new Path(treeFile.getPath()), treePath);
  }

  private static Job growSubtreesJob(String[] args, Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job growSubtreesJob = new Job(conf, "grow subtrees");
    growSubtreesJob.setJarByClass(TreeBuilder.class);
    growSubtreesJob.setMapperClass(GrowSubtreesJob.Map.class);
    growSubtreesJob.setReducerClass(GrowSubtreesJob.Reduce.class);

    growSubtreesJob.setMapOutputKeyClass(Text.class);
    growSubtreesJob.setMapOutputValueClass(Text.class);
    growSubtreesJob.setOutputKeyClass(NullWritable.class);
    growSubtreesJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(growSubtreesJob, inputPath);
    FileOutputFormat.setOutputPath(growSubtreesJob, outputPath);

    return growSubtreesJob;
  }

  private static Job filterInstancesJob(String[] args, Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job filterInstancesJob = new Job(conf, "filter training instances");
    filterInstancesJob.setJarByClass(TreeBuilder.class);
    filterInstancesJob.setMapperClass(FilterInstancesJob.Map.class);
    filterInstancesJob.setReducerClass(Reducer.class);

    filterInstancesJob.setMapOutputKeyClass(NullWritable.class);
    filterInstancesJob.setMapOutputValueClass(Text.class);
    filterInstancesJob.setOutputKeyClass(LongWritable.class);
    filterInstancesJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(filterInstancesJob, inputPath);
    FileOutputFormat.setOutputPath(filterInstancesJob, outputPath);

    return filterInstancesJob;
  }

  private static Job findBestFieldSplitJob(String[] args, Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job fieldSplitJob = new Job(conf, "best field splits");
    fieldSplitJob.setJarByClass(TreeBuilder.class);
    fieldSplitJob.setMapperClass(NodeSplitsJob.Map.class);
    fieldSplitJob.setReducerClass(NodeSplitsJob.Reduce.class);

    fieldSplitJob.setMapOutputKeyClass(Text.class);
    fieldSplitJob.setMapOutputValueClass(Text.class);
    fieldSplitJob.setOutputKeyClass(NullWritable.class);
    fieldSplitJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(fieldSplitJob, inputPath);
    FileOutputFormat.setOutputPath(fieldSplitJob, outputPath);

    return fieldSplitJob;
  }

  private static Job findBestCategorySplitJob(String[] args, Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job categorySplitJob = new Job(conf, "best category splits");
    categorySplitJob.setJarByClass(TreeBuilder.class);
    categorySplitJob.setMapperClass(NodeFieldSplitsJob.Map.class);
    categorySplitJob.setReducerClass(NodeFieldSplitsJob.Reduce.class);

    categorySplitJob.setMapOutputKeyClass(Text.class);
    categorySplitJob.setMapOutputValueClass(Text.class);
    categorySplitJob.setOutputKeyClass(NullWritable.class);
    categorySplitJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(categorySplitJob, inputPath);
    FileOutputFormat.setOutputPath(categorySplitJob, outputPath);

    return categorySplitJob;
  }

  private static Job setupDefineFieldsJob(String[] args, Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job defineFieldsJob = new Job(conf, "define fields");
    defineFieldsJob.setJarByClass(TreeBuilder.class);
    defineFieldsJob.setMapperClass(DefineFieldsJob.Map.class);
    defineFieldsJob.setReducerClass(DefineFieldsJob.Reduce.class);
    defineFieldsJob.setMapOutputKeyClass(IntWritable.class);
    defineFieldsJob.setMapOutputValueClass(Text.class);

    defineFieldsJob.setOutputKeyClass(NullWritable.class);
    defineFieldsJob.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(defineFieldsJob, inputPath);
    FileOutputFormat.setOutputPath(defineFieldsJob, outputPath);

    return defineFieldsJob;
  }

  private static ArrayList<Node> readSubtrees(Configuration conf, Path inputPath, int objectiveFieldIndex) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] ls = fs.listStatus(inputPath);

    ArrayList<String> allLines = new ArrayList<String>();
    for (FileStatus fileStatus : ls) {
      if (fileStatus.getPath().getName().startsWith("part")) {
        FSDataInputStream in = fs.open(fileStatus.getPath());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out, conf);
        in.close();
        out.close();

        String[] lines = out.toString().split("\n");
        allLines.addAll(Arrays.asList(lines));
      }
    }

    ArrayList<Node> subtrees = new ArrayList<Node>();

    SAXBuilder builder = new SAXBuilder();
    for (String line : allLines) {
      if (line.isEmpty()) {
        continue;
      }

      Reader in = new StringReader(line);

      Element subtreeElement = builder.build(in).getRootElement();
      Node subtree = Node.fromElement(subtreeElement, objectiveFieldIndex, null);
      subtrees.add(subtree);
    }

    return subtrees;
  }

  private static ArrayList<Field> readFieldDefinitions(Configuration conf, Path inputPath) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] ls = fs.listStatus(inputPath);

    ArrayList<String> allLines = new ArrayList<String>();
    for (FileStatus fileStatus : ls) {
      if (fileStatus.getPath().getName().startsWith("part")) {
        FSDataInputStream in = fs.open(fileStatus.getPath());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out, conf);
        in.close();
        out.close();

        String[] lines = out.toString().split("\n");
        allLines.addAll(Arrays.asList(lines));
      }
    }

    ArrayList<Field> allFields = new ArrayList<Field>();

    SAXBuilder builder = new SAXBuilder();
    for (String line : allLines) {
      if (line.isEmpty()) {
        continue;
      }

      Reader in = new StringReader(line);

      Element fieldElement = builder.build(in).getRootElement();
      Field field = Field.fromElement(fieldElement);
      allFields.add(field);
    }

    Collections.sort(allFields);

    return allFields;
  }

  private static BuildResults readNewSplits(Tree tree, HashMap<Integer, Node> nodeMap, Configuration conf, Path inputPath) throws Exception {
    boolean grewTree = false;
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] ls = fs.listStatus(inputPath);

    ArrayList<String> allLines = new ArrayList<String>();
    for (FileStatus fileStatus : ls) {
      if (fileStatus.getPath().getName().startsWith("part")) {
        FSDataInputStream in = fs.open(fileStatus.getPath());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out, conf);
        in.close();
        out.close();

        String[] lines = out.toString().split("\n");
        allLines.addAll(Arrays.asList(lines));
      }
    }

    long newLeafInstanceCount = 0;
    for (String line : allLines) {
      if (line.isEmpty()) {
        continue;
      }

      // 0,2,3.2727272727272725,0.9182958340544894,grape@2;apple@0;peach@0,grape@0;apple@2;peach@2

      String[] tokens = line.split(",");
      int nodeId = Integer.valueOf(tokens[0]);

      Node node = nodeMap.get(nodeId);

      double informationGain = Double.valueOf(tokens[3]);

      if (informationGain <= 0.0) {
        if (!node.isLeaf()) {
          newLeafInstanceCount += node.getTotalCount();
          node.setIsLeaf(true);
        }
      } else {
        int fieldId = Integer.valueOf(tokens[1]);
        Field field = tree.getFields().get(fieldId);

        String splitValueString = tokens[2];
        Split split;
        if (field.isCategorical()) {
          split = new Split(fieldId, splitValueString);
        } else {
          double splitNumber = Double.valueOf(splitValueString);
          split = new Split(fieldId, splitNumber);
        }

        TreeMap<String, Long> trueChildClassCounts = getCategoryCounts(tokens[4]);
        TreeMap<String, Long> falseChildClassCounts = getCategoryCounts(tokens[5]);

        Node trueChild = new Node(nodeMap.size(), node, trueChildClassCounts);
        nodeMap.put(trueChild.getId(), trueChild);
        Node falseChild = new Node(nodeMap.size(), node, falseChildClassCounts);
        nodeMap.put(falseChild.getId(), falseChild);

        node.addSplit(split, trueChild, falseChild);
        grewTree = true;

        System.out.println("ADAM - New Split: " + line);

        if (trueChild.getTotalCount() < Utils.DEFAULT_SUBTREE_FLOOR) {
          newLeafInstanceCount += trueChild.getTotalCount();
        }
        if (falseChild.getTotalCount() < Utils.DEFAULT_SUBTREE_FLOOR) {
          newLeafInstanceCount += falseChild.getTotalCount();
        }

      }

    }

    return new BuildResults(grewTree, newLeafInstanceCount);
  }

  private static TreeMap<String, Long> getCategoryCounts(String categoryCountString) {
    TreeMap<String, Long> countMap = new TreeMap<String, Long>();
    String[] categoryCountTokens = categoryCountString.split(";");

    for (String categoryCountToken : categoryCountTokens) {
      String[] tokens = categoryCountToken.split("@");
      String category = tokens[0];
      Long count = Long.valueOf(tokens[1]);
      countMap.put(category, count);
    }

    return countMap;
  }
}
