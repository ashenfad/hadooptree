package hadooptree;

import hadooptree.job.DefineFieldsJob;
import hadooptree.tree.Tree;
import hadooptree.tree.Field;
import hadooptree.tree.Node;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import static org.mockito.Mockito.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for DefineFieldsJob
 */
public class DefineFieldsTest {

  @Test
  public void testMap() throws Exception {
    DefineFieldsJob.Map fieldDefinerMap = new DefineFieldsJob.Map();

    Mapper.Context context = mock(Mapper.Context.class);
    fieldDefinerMap.map(null, new Text("apple, red, 9"), context);

    verify(context).write(new IntWritable(0), new Text("apple"));
    verify(context).write(new IntWritable(1), new Text("red"));
    verify(context).write(new IntWritable(2), new Text("9"));
  }

//  @Test
//  public void testReduce() throws Exception {
//    DefineFieldsJob.Reduce fieldDefinerReduce = new DefineFieldsJob.Reduce();
//
//    Reducer.Context categoricalContext = mock(Reducer.Context.class);
//    IntWritable categoricalKey = new IntWritable(0);
//    ArrayList<Text> categoricalValues = new ArrayList<Text>();
//    categoricalValues.add(new Text("apple"));
//    categoricalValues.add(new Text("orange"));
//    categoricalValues.add(new Text("orange"));
//    categoricalValues.add(new Text("pear"));
//    categoricalValues.add(new Text("apple"));
//    categoricalValues.add(new Text("orange"));
//
//    fieldDefinerReduce.reduce(categoricalKey, categoricalValues, categoricalContext);
//
//    Text categoricalResultText = new Text("0,true,apple,orange,pear");
//    verify(categoricalContext).write(NullWritable.get(), categoricalResultText);
//
//    Reducer.Context numericContext = mock(Reducer.Context.class);
//    IntWritable numericKey = new IntWritable(1);
//    ArrayList<Text> numericValues = new ArrayList<Text>();
//    numericValues.add(new Text("4.8"));
//    numericValues.add(new Text("6.2"));
//    numericValues.add(new Text("1.6"));
//    numericValues.add(new Text("3.2"));
//
//    fieldDefinerReduce.reduce(numericKey, numericValues, numericContext);
//
//    Text numericResultText = new Text("1,false,1.6,6.2");
//    verify(numericContext).write(NullWritable.get(), numericResultText);
//  }
  @Test
  public void testFieldDefinition() throws Exception {
    /* Test categorical field */
    HashSet<String> categories = new HashSet<String>(Arrays.asList("blue", "green", "red"));

    Field categoricalField = new Field(0);
    for (String category : categories) {
      categoricalField.addCategoricalValue(category);
    }

    Element categoricalElement = categoricalField.toElement();
    Field reloadedCategoricalField = Field.fromElement(categoricalElement);
    Assert.assertEquals(reloadedCategoricalField, categoricalField);


    /* Test numeric field */
    HashSet<Double> numbers = new HashSet<Double>(Arrays.asList(2.3, 1.8, 6.2, 2.5));

    Field numericField = new Field(0);
    for (Double number : numbers) {
      numericField.addNumericValue(number);
    }

    Element numericElement = numericField.toElement();
    Field reloadedNumericField = Field.fromElement(numericElement);

    System.out.println(numericField);
    System.out.println(reloadedNumericField);

    Assert.assertEquals(numericField, reloadedNumericField);


    ArrayList<Field> fields = new ArrayList<Field>();
    fields.add(categoricalField);
    fields.add(numericField);
    Node root = new Node(0, null);
    Tree tree = new Tree(root, fields, 0);

    Element treeElement = tree.toElement();
    Tree newTree = Tree.fromElement(treeElement);
    Assert.assertEquals(tree, newTree);

  }
}
