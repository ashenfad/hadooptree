package hadooptree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class EvaluateTree {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("Expected: java -jar HadoopTree.jar <treeXML> <testCSV>");
      return;
    }


    SAXBuilder saxBuilder = new SAXBuilder();
    Reader xmlIn = new FileReader(args[0]);

    Element treeElement = saxBuilder.build(xmlIn).getRootElement();
    Tree tree = Tree.fromElement(treeElement);

    BufferedReader reader = new BufferedReader(new FileReader(args[1]));
    long errors = 0;
    long total = 0;

    while (reader.ready()) {
      String line = reader.readLine();
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }

      String[] tokens = line.split(",");
      ArrayList<Object> instance = new ArrayList<Object>();

      for (String token : tokens) {
        Object value;
        try {
          value = Double.valueOf(token);
        } catch (NumberFormatException e) {
          value = token;
        }
        instance.add(value);
      }

      Node node = tree.evalToNode(instance);
      String predictedClass = node.getPredictedClass();
      String actualClass = (String) instance.get(tree.getObjectiveFieldIndex());

      if (!predictedClass.equals(actualClass)) {
        errors++;
      }

      total++;
    }

    long correct = total - errors;
    double successRate = (double) correct / (double) total;
    successRate *= 100;
    System.out.println("Result: " + correct + "/" + total + " --- " + successRate);

  }
}
