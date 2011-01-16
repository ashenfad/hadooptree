package hadooptree.tree;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

public class Field implements Comparable<Field> {

  private int index;
  private Boolean isCategorical;
  private double minValue;
  private double maxValue;
  private double sum;
  private long count;
  private TreeMap<String, Long> categoryMap;

  public Field(int index) {
    this.index = index;
    minValue = Double.MAX_VALUE;
    maxValue = -Double.MAX_VALUE;
    categoryMap = new TreeMap<String, Long>();
    isCategorical = null;
    sum = 0;
    count = 0;
  }

  public int getIndex() {
    return index;
  }

  public boolean isCategorical() {
    return isCategorical;
  }

  public double getMaxValue() {
    return maxValue;
  }

  public double getMinValue() {
    return minValue;
  }

  public double getAverage() {
    return sum / (double) count;
  }

  public Set<String> getCategorySet() {
    return categoryMap.keySet();
  }

  public TreeMap<String, Long> getCategoryMap() {
    return categoryMap;
  }

  public String getMostCommonCategory() {
    String mostCommonCategory = null;
    long maxCategoryCount = -Long.MAX_VALUE;
    for (Entry<String, Long> entry : categoryMap.entrySet()) {
      String category = entry.getKey();
      Long categoryCount = entry.getValue();

      if (categoryCount > maxCategoryCount) {
        mostCommonCategory = category;
        maxCategoryCount = categoryCount;
      }
    }
    return mostCommonCategory;
  }

  public void addCategoricalValue(String category) throws Exception {
    if (isCategorical != null && !isCategorical) {
      throw new Exception("Field " + index + " is numeric.  Can't accept: " + category);
    } else {
      if (isCategorical == null) {
        isCategorical = true;
      }
      incrementCategory(category);
      this.count++;
    }
  }

  public void addNumericValue(double number) throws Exception {
    if (isCategorical != null && isCategorical) {
      throw new Exception("Field " + index + " is categorical.  Can't accept: " + number);
    } else {
      if (isCategorical == null) {
        isCategorical = false;
      }
      minValue = Math.min(number, minValue);
      maxValue = Math.max(number, maxValue);
      sum += number;
      this.count++;
    }
  }

  public Element toElement() {
    Element element = new Element("field");
    element.setAttribute("index", String.valueOf(index));
    element.setAttribute("isCategorical", String.valueOf(isCategorical));
    if (isCategorical) {
      for (Entry<String, Long> entry : categoryMap.entrySet()) {
        String category = entry.getKey();
        Long categoryCount = entry.getValue();
        Element categoryElement = new Element("category");
        categoryElement.setAttribute("value", category);
        categoryElement.setAttribute("count", String.valueOf(categoryCount));
        element.addContent(categoryElement);
      }
    } else {
      element.setAttribute("minValue", String.valueOf(minValue));
      element.setAttribute("maxValue", String.valueOf(maxValue));
      element.setAttribute("sum", String.valueOf(sum));
    }
    element.setAttribute("count", String.valueOf(count));

    return element;
  }

  private void incrementCategory(String category) {
    Long categoryCount = categoryMap.get(category);
    if (categoryCount == null) {
      categoryCount = 1l;
    } else {
      categoryCount++;
    }
    categoryMap.put(category, categoryCount);
  }

  private void setCategoryEntry(String category, Long categoryCount) {
    categoryMap.put(category, categoryCount);
  }

  private void setNumericInfo(double minValue, double maxValue, double sum) {
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.sum = sum;
  }

  private void setCount(long count) {
    this.count = count;
  }

  private void setCategorical(boolean isCategorical) {
    this.isCategorical = isCategorical;
  }

  public static Field fromElement(Element element) throws Exception {
    int index = Integer.valueOf(element.getAttributeValue("index"));
    Field field = new Field(index);

    long count = Long.valueOf(element.getAttributeValue("count"));
    field.setCount(count);

    boolean isCategorical = Boolean.valueOf(element.getAttributeValue("isCategorical"));
    field.setCategorical(isCategorical);

    if (isCategorical) {
      List<Element> children = (List<Element>) element.getChildren("category");
      for (Element child : children) {
        String categoryValue = child.getAttributeValue("value");
        Long categoryCount = Long.valueOf(child.getAttributeValue("count"));
        field.setCategoryEntry(categoryValue, categoryCount);
      }
    } else {
      double minValue = Double.valueOf(element.getAttributeValue("minValue"));
      double maxValue = Double.valueOf(element.getAttributeValue("maxValue"));
      double sum = Double.valueOf(element.getAttributeValue("sum"));

      field.setNumericInfo(minValue, maxValue, sum);
    }

    return field;
  }

  @Override
  public String toString() {
    XMLOutputter outputter = new XMLOutputter();
    String output = outputter.outputString(this.toElement());
    return output;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Field other = (Field) obj;
    if (this.index != other.index) {
      return false;
    }
    if (this.isCategorical != other.isCategorical && (this.isCategorical == null || !this.isCategorical.equals(other.isCategorical))) {
      return false;
    }
    if (Double.doubleToLongBits(this.minValue) != Double.doubleToLongBits(other.minValue)) {
      return false;
    }
    if (Double.doubleToLongBits(this.maxValue) != Double.doubleToLongBits(other.maxValue)) {
      return false;
    }
    if (Double.doubleToLongBits(this.sum) != Double.doubleToLongBits(other.sum)) {
      return false;
    }
    if (this.count != other.count) {
      return false;
    }
    if (this.categoryMap != other.categoryMap && (this.categoryMap == null || !this.categoryMap.equals(other.categoryMap))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 61 * hash + this.index;
    hash = 61 * hash + (this.isCategorical != null ? this.isCategorical.hashCode() : 0);
    hash = 61 * hash + (int) (Double.doubleToLongBits(this.minValue) ^ (Double.doubleToLongBits(this.minValue) >>> 32));
    hash = 61 * hash + (int) (Double.doubleToLongBits(this.maxValue) ^ (Double.doubleToLongBits(this.maxValue) >>> 32));
    hash = 61 * hash + (int) (Double.doubleToLongBits(this.sum) ^ (Double.doubleToLongBits(this.sum) >>> 32));
    hash = 61 * hash + (int) (this.count ^ (this.count >>> 32));
    hash = 61 * hash + (this.categoryMap != null ? this.categoryMap.hashCode() : 0);
    return hash;
  }

  @Override
  public int compareTo(Field t) {
    return Integer.valueOf(this.getIndex()).compareTo(Integer.valueOf(t.getIndex()));
  }
}
