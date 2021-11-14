package de.tum.i13.server.persistentStorage.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// A BTree
class PersistentBtree<V> {
  public BTreeNode<V> root; // Pointer to root node
  public int minimumDegree; // Minimum degree

  // Constructor (Initializes tree as empty)
  PersistentBtree(int minimumDegree) {
    this.root = null;
    this.minimumDegree = minimumDegree;
  }

  // function to traverse the tree
  void traverse() {
    if (this.root != null)
      this.root.traverse();
    System.out.println();
  }

  // function to search a key in this tree
  V search(String key) {
    if (this.root == null)
      return null;
    else
      return this.root.search(key);
  }

  // The main function that inserts a new key in this B-Tree
  void insert(String key, V value) {
    // If tree is empty
    if (root == null) {
      // Create new node
      try {
        root = new BTreeNode<V>(minimumDegree, true);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
      root.chunk.set(0, new Pair<V>(key, value));
      root.incrementKeyCount(); // Update number of keys in root

      return;
    }
  
    // If tree is not empty
    // If root is full, then tree grows in height
    if (root.getKeyCount() == 2 * this.minimumDegree - 1) {
      // Allocate memory for new root
      BTreeNode<V> s;
      try {
        s = new BTreeNode<V>(this.minimumDegree, false);
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }

      // Make old root as child of new root
      s.getChildren().set(0, root);

      // Split the old root and move 1 key to the new root
      s.splitChild(0, root);

      // New root has two children now. Decide which of the
      // two children is going to have new key
      int i = 0;
      if (s.chunk.get(0).key.compareTo(key) < 0)
        i++;
      s.getChildren().get(i).insertNonFull(key, value);

      // Change root
      root = s;
    } else // If root is not full, call insertNonFull for root
      root.insertNonFull(key, value);
  }
}

// A BTree node
class BTreeNode<V> {
  private String fileName;
  private int minimumDegree; // Minimum degree (defines the range for number of keys)
  private List<BTreeNode<V>> children; // An array of child pointers
  private int keyCount; // Current number of keys
  public boolean leaf; // Is true when node is leaf. Otherwise false
  public Chunk<V> chunk;

  // Constructor
  BTreeNode(int t, boolean leaf) throws Exception {
    this.minimumDegree = t;
    this.leaf = leaf;
    this.children = new ArrayList<BTreeNode<V>>(Collections.nCopies((2 * minimumDegree), null));
    this.keyCount = 0;
    this.fileName = Integer.toString(this.hashCode());
    this.chunk = new ChunkProxy<V>(fileName, this.minimumDegree);
  }

  BTreeNode(int t, boolean leaf, Pair<V> rootElement) throws Exception {
    this.minimumDegree = t;
    this.leaf = leaf;
    this.children = new ArrayList<BTreeNode<V>>(Collections.nCopies((2 * minimumDegree), null));
    this.keyCount = 0;
    this.fileName = Integer.toString(this.hashCode());
    this.chunk = new ChunkProxy<V>(fileName, this.minimumDegree, Arrays.asList(rootElement) );
  }


  // A function to traverse all nodes in a subtree rooted with this node
  void traverse() {

    // There are n keys and n+1 children, traverse through n keys
    // and first n children
    int i = 0;
    for (i = 0; i < this.keyCount; i++) {

      // If this is not leaf, then before printing key[i],
      // traverse the subtree rooted with child C[i].
      if (this.leaf == false) {
        this.children.get(i).traverse();
      }

      Pair<V> pair = this.chunk.get(i);

      System.out.println(pair.key + " -> " + pair.value );
    }

    // Print the subtree rooted with last child
    if (leaf == false)
      this.children.get(i).traverse();
  }

  // A function to search a key in the subtree rooted with this node.
  V search(String k) { // returns NULL if k is not present.
    // Find the first key greater than or equal to k
    // int i = chunk.findIndexOfFirstGreaterThen(k);
    Pair<V> value = chunk.findValueOfFirstGreaterThen(k);

    // If the found key is equal to k, return this node
    if (value.key.equals(k))
      return value.value;

    // If the key is not found here and this is a leaf node
    if (leaf == true)
      return null;

    // Go to the appropriate child
    return children.get(this.chunk.lastIndexRead()).search(k);
  }

  // A utility function to insert a new key in this node
  // The assumption is, the node must be non-full when this
  // function is called
  void insertNonFull(String key, V value) {
    // Initialize index as index of rightmost element
    int i = keyCount - 1;

    // If this is a leaf node
    if (leaf == true) {
      // The following loop does two things
      // a) Finds the location of new key to be inserted
      // b) Moves all greater keys to one place ahead

      i = chunk.shiftRightOneAfterFirstGreaterThan(key);

      // Insert the new key at found location
      chunk.set(i + 1, new Pair<V>(key, value));
      keyCount++;
    } else // If this node is not leaf
    {
      // Find the child which is going to have the new key
      while (i >= 0 && chunk.get(i).key.compareTo(key) > 0)
        i--;

      BTreeNode<V> child = this.children.get(i + 1);

      // See if the found child is full
      if (child.keyCount == 2 * this.minimumDegree - 1) {
        // If the child is full, then split it
        splitChild(i + 1, child);

        // After split, the middle key of C[i] goes up and
        // C[i] is splitted into two. See which of the two
        // is going to have the new key
        if (chunk.get(i + 1).key.compareTo(key) < 0)
          i++;
      }
      this.children.get(i + 1).insertNonFull(key, value);
    }
  }

  // A utility function to split the child y of this node
  // Note that y must be full when this function is called
  void splitChild(int i, BTreeNode<V> child) {
    // Create a new node which is going to store (t-1) keys
    // of y
    BTreeNode<V> newNode;
    try {
      newNode = new BTreeNode<V>(child.minimumDegree, child.leaf);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    newNode.keyCount = this.minimumDegree - 1;

    // Copy the last (t-1) keys of y to z
    for (int j = 0; j < this.minimumDegree - 1; j++) {
      newNode.chunk.set(j, child.chunk.remove(j + this.minimumDegree));
    }

    // Copy the last t children of y to z
    if (child.leaf == false) {
      for (int j = 0; j < this.minimumDegree; j++) {
        newNode.children.set(j, child.children.get(j + this.minimumDegree));
      }
    }

    // Reduce the number of keys in y
    child.keyCount = this.minimumDegree - 1;

    // Since this node is going to have a new child,
    // create space of new child
    for (int j = this.keyCount; j >= i + 1; j--)
      this.children.set(j + 1, this.children.get(j));

    // Link the new child to this node
    this.children.set(i + 1, newNode);

    // A key of y will move to this node. Find the location of
    // new key and move all greater keys one space ahead
    this.chunk.shiftRightOne(i);

    // Copy the middle key of y to this node
    this.chunk.set(i, child.chunk.remove(this.minimumDegree - 1));

    // Increment count of keys in this node
    this.keyCount++;
  }

  List<BTreeNode<V>> getChildren() {
    return this.children;
  }

  int getKeyCount() {
    return this.keyCount;
  }

  int incrementKeyCount() {
    this.keyCount++;
    return this.keyCount;
  }

  public static void main(String[] args) {
    PersistentBtree<String> t = new PersistentBtree<String>(3); // A B-Tree with minimum degree 3
    t.insert("78", "a");
    t.insert("52", "b");
    t.insert("81", "c");
    t.insert("40", "d");
    t.insert("33", "e");
    t.insert("90", "f");
    t.insert("35", "g");
    t.insert("20", "h");
    t.insert("52", "c");
    t.insert("38", "h");

    System.out.println("Traversal of the constructed tree is ");
    t.traverse();

    String k = "52";

    if (t.search(k) != null) {
      System.out.println("\nPresent");
    } else {
      System.out.println("\nNot Present");
    }

    k = "15";
    if (t.search(k) != null)
      System.out.println("\nPresent");
    else
      System.out.println("\nNot Present");
  }
}
