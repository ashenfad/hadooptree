package hadooptree;

import hadooptree.tree.Split;

public class SplitResults {

  private final Split split;
  private final double informationGain;
  private final Long[] trueCounts;
  private final Long[] falseCounts;

  public SplitResults(Split split, double informationGain, Long[] trueCounts, Long[] falseCounts) {
    this.split = split;
    this.informationGain = informationGain;
    this.trueCounts = trueCounts;
    this.falseCounts = falseCounts;
  }

  public Split getSplit() {
    return split;
  }

  public double getInformationGain() {
    return informationGain;
  }

  public Long[] getTrueCounts() {
    return trueCounts;
  }

  public Long[] getFalseCounts() {
    return falseCounts;
  }
}
