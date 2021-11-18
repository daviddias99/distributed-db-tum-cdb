package de.tum.i13.server.persistentStorage.btree;

import java.util.Set;
import java.util.TreeSet;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.Pair;

import java.util.List;

public class TreeValidator {
  public static boolean validTree(PersistentBTree<String> tree) {

    Set<String> keys = new TreeSet<>();

    if (tree.root == null) {
      return true;
    }

    return validTreeRec(tree.root, tree.minimumDegree, keys);
  }

  private static boolean validTreeRec(PersistentBTreeNode<String> node, int minimumDegree, Set<String> keys) {
    int keyCount = node.getKeyCount();
    Chunk<String> chunk = node.getChunk();
    List<Pair<String>> chunkElements = chunk.getKeys();

    for (Pair<String> pair : chunkElements) {
      if (pair == null)
        break;

      boolean success = keys.add(pair.key);

      if (!success) {
        return false;
      }
    }

    if (keyCount != chunk.getKeyCount()) {
      return false;
    }

    if (node.getChildrenCount() > 2 * minimumDegree) {
      return false;
    }

    for (PersistentBTreeNode<String> child : node.getChildren()) {

      if (child == null) {
        break;
      }

      boolean res = validTreeRec(child, minimumDegree, keys);

      if (!res) {
        return false;
      }
    }

    return true;
  }

}
