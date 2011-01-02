package hadooptree;

import hadooptree.tree.Field;
import hadooptree.tree.Tree;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

public class Utils {

//  public static final int DEFAULT_NUMERIC_SPLITS = 2;
//  public static final int DEFAULT_SPLIT_FLOOR = 0;
  public static final int DEFAULT_NUMERIC_SPLITS = 10000;
  public static final int DEFAULT_SPLIT_FLOOR = 10;

  public static ArrayList<Object> convertInstanceStringToArrayList(String instanceString, ArrayList<Field> fields) throws Exception {
    ArrayList<Object> values = new ArrayList<Object>();

    String[] tokens = instanceString.split(",");
    if (tokens.length != fields.size()) {
      throw new Exception("instanceString has " + tokens.length + " tokens instead of the expected " + fields.size() + " --- instance: " + instanceString);
    }

    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      token = token.trim();
      if (fields.get(i).isCategorical()) {
        values.add(token);
      } else {
        values.add(Double.valueOf(token));
      }
    }

    return values;
  }

  public static Tree loadTree(Configuration conf) throws Exception {
    URI[] files = DistributedCache.getCacheFiles(conf);

    Path path = new Path(files[0].toString());

    FileSystem fs = FileSystem.get(conf);
    FSDataInputStream in = fs.open(path);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    IOUtils.copyBytes(in, out, conf);
    in.close();
    out.close();
    String[] lines = out.toString().split("\n");

    StringBuilder stringBuilder = new StringBuilder();
    for (String line : lines) {
      stringBuilder.append(line);
    }

    String treeXml = stringBuilder.toString();

    SAXBuilder saxBuilder = new SAXBuilder();

    Reader xmlIn = new StringReader(treeXml);

    Tree tree = null;
    try {
      Element treeElement = saxBuilder.build(xmlIn).getRootElement();
      tree = Tree.fromElement(treeElement);
    } catch (Exception e) {
      throw new IOException("TreeXml: " + treeXml, e);
    }

    return tree;
  }
}
