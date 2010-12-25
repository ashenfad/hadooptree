package hadooptree;

import java.util.ArrayList;
import org.jdom.Element;

public class Split {

  private final int fieldId;
  private final boolean isCategorical;
  private final String category;
  private final Double number;

  public Split(int fieldId, String category) {
    this.fieldId = fieldId;
    this.category = category;
    isCategorical = true;
    number = null;
  }

  public Split(int fieldId, Double number) {
    this.fieldId = fieldId;
    this.number = number;
    isCategorical = false;
    category = null;
  }

  public int getFieldId() {
    return fieldId;
  }

  public Double getNumber() {
    return number;
  }
  
  public boolean eval(ArrayList<Object> instance) {
    boolean result;
    Object fieldValue = instance.get(fieldId);
    if (isCategorical) {
      result = ((String) fieldValue).equals(category);
    } else {
      result = ((Double) fieldValue) <= number;
    }
    return result;
  }

  public Element toElement() {
    Element element = new Element("split");
    element.setAttribute("fieldId", String.valueOf(fieldId));
    element.setAttribute("isCategorical", String.valueOf(isCategorical));
    if (isCategorical) {
      element.setAttribute("equalTo", category);
    } else {
      element.setAttribute("lessOrEqualTo", String.valueOf(number));
    }

    return element;
  }

  public static Split fromElement(Element element) {
    int fieldId = Integer.valueOf(element.getAttributeValue("fieldId"));
    boolean isCategorical = Boolean.valueOf(element.getAttributeValue("isCategorical"));

    Split split;
    if (isCategorical) {
      String category = element.getAttributeValue("equalTo");
      split = new Split(fieldId, category);
    } else {
      String numberString = element.getAttributeValue("lessOrEqualTo");
      double splitNumber = Double.valueOf(numberString);
      split = new Split(fieldId, splitNumber);
    }

    return split;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Split other = (Split) obj;
    if (this.fieldId != other.fieldId) {
      return false;
    }
    if (this.isCategorical != other.isCategorical) {
      return false;
    }
    if ((this.category == null) ? (other.category != null) : !this.category.equals(other.category)) {
      return false;
    }
    if (this.number != other.number && (this.number == null || !this.number.equals(other.number))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 5;
    hash = 43 * hash + this.fieldId;
    hash = 43 * hash + (this.category != null ? this.category.hashCode() : 0);
    hash = 43 * hash + (this.number != null ? this.number.hashCode() : 0);
    return hash;
  }
}
