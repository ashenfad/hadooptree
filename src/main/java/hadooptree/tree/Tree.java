package hadooptree.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

public class Tree {

  private final int objectiveFieldIndex;
  private final ArrayList<Field> fields;
  private Node root;

  public Tree(Node root, ArrayList<Field> fields, int objectiveFieldIndex) throws Exception {
    this.objectiveFieldIndex = objectiveFieldIndex;
    this.fields = fields;
    this.root = root;

    Field objectiveField = fields.get(objectiveFieldIndex);
    if (!objectiveField.isCategorical()) {
      throw new Exception("Numeric objective not supported yet");
    }
  }

  public ArrayList<Field> getFields() {
    return fields;
  }

  public int getObjectiveFieldIndex() {
    return objectiveFieldIndex;
  }

  public Field getObjectiveField() {
    return fields.get(objectiveFieldIndex);
  }

  public Node evalToNode(ArrayList<Object> instance) {
    return root.evalToNode(instance);
  }

  public HashMap<String, Integer> createObjectiveCategoryIdMap() {
    ArrayList<String> objectiveCategories = new ArrayList<String>(this.getObjectiveField().getCategorySet());
    HashMap<String, Integer> categoryIdMap = new HashMap<String, Integer>();
    for (int i = 0; i < objectiveCategories.size(); i++) {
      categoryIdMap.put(objectiveCategories.get(i), i);
    }
    return categoryIdMap;
  }

  public Element toElement() {
    Element treeElement = new Element("tree");
    treeElement.setAttribute("objectiveFieldIndex", String.valueOf(objectiveFieldIndex));

    Element fieldsElement = new Element("fields");
    for (Field field : fields) {
      fieldsElement.addContent(field.toElement());
    }
    treeElement.addContent(fieldsElement);

    treeElement.addContent(root.toElement("root"));

    return treeElement;
  }

  @Override
  public String toString() {
    XMLOutputter outputter = new XMLOutputter();
    String output = outputter.outputString(this.toElement());
    return output;
  }

  public static Tree fromElement(Element treeElement) throws Exception {
    int objectiveFieldIndex = Integer.valueOf(treeElement.getAttributeValue("objectiveFieldIndex"));

    Element rootElement = treeElement.getChild("root");
    Node root = Node.fromElement(rootElement, objectiveFieldIndex, null);

    ArrayList<Field> fields = null;
    Element fieldsElement = treeElement.getChild("fields");
    fields = new ArrayList<Field>();
    List<Element> fieldElements = (List<Element>) fieldsElement.getChildren();
    for (Element fieldElement : fieldElements) {
      Field field = Field.fromElement(fieldElement);
      fields.add(field);
    }

    Tree tree = new Tree(root, fields, objectiveFieldIndex);

    return tree;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Tree other = (Tree) obj;
    if (this.objectiveFieldIndex != other.objectiveFieldIndex) {
      return false;
    }
    if (this.fields != other.fields && (this.fields == null || !this.fields.equals(other.fields))) {
      return false;
    }
    if (this.root != other.root && (this.root == null || !this.root.equals(other.root))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 11 * hash + this.objectiveFieldIndex;
    hash = 11 * hash + (this.fields != null ? this.fields.hashCode() : 0);
    hash = 11 * hash + (this.root != null ? this.root.hashCode() : 0);
    return hash;
  }
}
