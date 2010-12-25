package hadooptree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.jdom.Element;

public class Node {

  private final int id;
  private final Node parent;
  private Node trueChild;
  private Node falseChild;
  private boolean isLeaf;
  private Split split;
  private TreeMap<String, Long> objectiveCategoryMap;

  public Node(int id, Node parent) {
    this.id = id;
    this.parent = parent;
    this.isLeaf = false;
  }

  public Node(int id, Node parent, TreeMap<String, Long> objectiveCategoryMap) {
    this(id, parent);
    this.objectiveCategoryMap = objectiveCategoryMap;
  }

  public Node getParent(Node parent) {
    return parent;
  }

  public double[] getRange(Node node, Field field) {
    if (field.isCategorical()) {
      return null;
    }

    double[] range;

    if (parent == null) {
      range = new double[2];
      range[0] = field.getMinValue();
      range[1] = field.getMaxValue();
    } else {
      range = parent.getRange(this, field);
      if (split != null && split.getFieldId() == field.getIndex()) {
        if (node == trueChild) {
          range[1] = Math.min(range[1], split.getNumber());
        } else if (node == falseChild) {
          range[0] = Math.max(range[0], split.getNumber());
        }
      }
    }
    return range;
  }

  public long getTotalCount() {
    long count = 0;
    for (Long categoryCount : objectiveCategoryMap.values()) {
      count += categoryCount;
    }
    return count;
  }

  public String getPredictedClass() {
    String predictedClass = null;
    long maxCount = 0;
    for (Entry<String, Long> entry : objectiveCategoryMap.entrySet()) {
      long count = entry.getValue();
      if (maxCount < count) {
        maxCount = count;
        predictedClass = entry.getKey();
      }
    }
    return predictedClass;
  }

  public Node evalToNode(ArrayList<Object> instance) {
    if (split == null) {
      return this;
    } else {
      if (split.eval(instance)) {
        return trueChild.evalToNode(instance);
      } else {
        return falseChild.evalToNode(instance);
      }
    }
  }

  public void setIsLeaf(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public int getId() {
    return id;
  }

  public void addSplit(Split split, Node trueChild, Node falseChild) {
    this.split = split;
    this.trueChild = trueChild;
    this.falseChild = falseChild;
  }

  public Element toElement(String nodeName) {
    Element element = new Element(nodeName);
    element.setAttribute("id", String.valueOf(id));
    element.setAttribute("isLeaf", String.valueOf(isLeaf));

    if (objectiveCategoryMap != null) {
      Element classCounts = new Element("classCounts");
      for (Entry<String, Long> entry : objectiveCategoryMap.entrySet()) {
        String category = entry.getKey();
        Long count = entry.getValue();
        Element classCount = new Element("classCount");
        classCount.setAttribute("classCategory", category);
        classCount.setAttribute("count", String.valueOf(count));
        classCounts.addContent(classCount);
      }
      element.addContent(classCounts);
    }

    if (split != null) {
      element.addContent(split.toElement());
      element.addContent(trueChild.toElement("trueChild"));
      element.addContent(falseChild.toElement("falseChild"));
    }

    return element;
  }

  public static Node fromElement(Element element, int objectiveFieldIndex, Node parent) throws Exception {
    int id = Integer.valueOf(element.getAttributeValue("id"));

    Element classCountsElement = element.getChild("classCounts");

    Node node;
    if (classCountsElement != null) {
      TreeMap<String, Long> classCountsMap = new TreeMap<String, Long>();
      List<Element> children = (List<Element>) classCountsElement.getChildren("classCount");
      for (Element classCountElement : children) {
        String category = classCountElement.getAttributeValue("classCategory");
        Long count = Long.valueOf(classCountElement.getAttributeValue("count"));
        classCountsMap.put(category, count);
      }
      node = new Node(id, parent, classCountsMap);
    } else {
      node = new Node(id, parent);
    }

    boolean isLeaf = Boolean.valueOf(element.getAttributeValue("isLeaf"));
    node.setIsLeaf(isLeaf);

    Element splitElement = element.getChild("split");
    if (splitElement != null) {
      Split split = Split.fromElement(splitElement);
      Node trueChild = Node.fromElement(element.getChild("trueChild"), objectiveFieldIndex, node);
      Node falseChild = Node.fromElement(element.getChild("falseChild"), objectiveFieldIndex, node);
      node.addSplit(split, trueChild, falseChild);
    }

    return node;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Node other = (Node) obj;
    if (this.id != other.id) {
      return false;
    }
    if (this.trueChild != other.trueChild && (this.trueChild == null || !this.trueChild.equals(other.trueChild))) {
      return false;
    }
    if (this.falseChild != other.falseChild && (this.falseChild == null || !this.falseChild.equals(other.falseChild))) {
      return false;
    }
    if (this.isLeaf != other.isLeaf) {
      return false;
    }
    if (this.split != other.split && (this.split == null || !this.split.equals(other.split))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 29 * hash + this.id;
    hash = 29 * hash + (this.trueChild != null ? this.trueChild.hashCode() : 0);
    hash = 29 * hash + (this.falseChild != null ? this.falseChild.hashCode() : 0);
    hash = 29 * hash + (this.split != null ? this.split.hashCode() : 0);
    return hash;
  }
}
