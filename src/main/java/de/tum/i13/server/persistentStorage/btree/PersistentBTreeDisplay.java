package de.tum.i13.server.persistentStorage.btree;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.Pair;

public class PersistentBTreeDisplay<V> {
  /*
   * Display functions
   */

  // function to traverse the tree
  void traverse(PersistentBTree<V> tree) {

    PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
    if (tree.root != null)
      nodeDisplay.traverse(tree.root);
    System.out.println();
  }

  void traverseCondensed(PersistentBTree<V> tree) {
    PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
    if (tree.root != null)
      nodeDisplay.traverseCondensed(tree.root);
    System.out.println();
  }

  void traverseSpecial(PersistentBTree<V> tree) {
    PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
    if (tree.root != null)
      nodeDisplay.traverseSpecial(tree.root);
    System.out.println();
  }

  private class PersistentBTreeNodeDisplay {
    // A function to traverse all nodes in a subtree rooted with this node
    void traverse(PersistentBTreeNode<V> node) {

      // There are n keys and n+1 children, traverse through n keys
      // and first n children
      int i = 0;
      for (i = 0; i < node.keyCount; i++) {

        // If this is not leaf, then before printing key[i],
        // traverse the subtree rooted with child C[i].
        if (node.leaf == false) {
          this.traverse(node.children.get(i));
        }

        Chunk<V> chunk = node.getChunk();

        if (chunk == null) {
          return;
        }

        Pair<V> pair = chunk.get(i);
        chunk = null;
        System.out.println(pair.key + " -> " + pair.value);
      }

      // Print the subtree rooted with last child
      if (node.leaf == false)
        this.traverse(node.children.get(i));
    }

    // A function to traverse all nodes in a subtree rooted with this node
    void traverseCondensed(PersistentBTreeNode<V> node) {

      // There are n keys and n+1 children, traverse through n keys
      // and first n children
      int i = 0;
      for (i = 0; i < node.keyCount; i++) {

        // If this is not leaf, then before printing key[i],
        // traverse the subtree rooted with child C[i].
        if (node.leaf == false) {
          this.traverseCondensed(node.children.get(i));
        }

        Chunk<V> chunk = node.getChunk();

        if (chunk == null) {
          return;
        }

        Pair<V> pair = chunk.get(i);
        chunk = null;
        System.out.print(pair.key + " ");
      }

      // Print the subtree rooted with last child
      if (node.leaf == false)
        this.traverseCondensed(node.children.get(i));
    }

    void traverseSpecial(PersistentBTreeNode<V> node) {

      System.out.println("--");
      System.out.println("Node(" + Integer.toString(node.id) + ")");
      System.out.println("Key count: " + node.keyCount);
      System.out.print("Keys: ");

      // There are n keys and n+1 children, traverse through n keys
      // and first n children
      int i = 0;
      Chunk<V> chunk = node.getChunk();
      for (i = 0; i < node.keyCount; i++) {
        if (chunk == null) {
          break;
        }

        Pair<V> pair = chunk.get(i);
        System.out.print(pair.key + " ");
      }
      chunk = null;
      System.out.print("\n");

      System.out.print("Children: ");

      for (PersistentBTreeNode<V> bTreeNode : node.children) {

        if (bTreeNode == null) {
          break;
        }

        System.out.print(Integer.toString(bTreeNode.id) + " ");
      }

      System.out.print("\n");

      for (PersistentBTreeNode<V> bTreeNode : node.children) {

        if (bTreeNode == null) {
          break;
        }

        this.traverseSpecial(bTreeNode);
      }
    }

  }

}
