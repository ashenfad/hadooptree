package hadooptree;

public class BuildResults {
    private final boolean grewTree;
    private final long leafInstanceCount;

    public BuildResults(boolean grewTree, long leafInstanceCount) {
      this.grewTree = grewTree;
      this.leafInstanceCount = leafInstanceCount;
    }

    public boolean isGrewTree() {
      return grewTree;
    }

    public long getLeafInstanceCount() {
      return leafInstanceCount;
    }
}
