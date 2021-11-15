package de.tum.i13.server.persistentStorage.btree;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// A BTree
class PersistentBtree<V> {
  public BTreeNode<V> root; // Pointer to root node
  public int minimumDegree; // Minimum degree
  public String storageFolder;

  // Constructor (Initializes tree as empty)
  PersistentBtree(int minimumDegree, String storageFolder) {
    this.root = null;
    this.minimumDegree = minimumDegree;
    this.storageFolder = storageFolder;
    this.createStorageFolder();
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
      this.createRoot(key, value);
      return;
    }

    // If root is full, then tree grows in height
    if (this.isRootFull()) {
      this.insertFull(key, value);
    } else // If root is not full, call insertNonFull for root
      root.insertNonFull(key, value);
  }

  private void createStorageFolder() {
    File theDir = new File(this.storageFolder);
    if (!theDir.exists()) {
      theDir.mkdirs();
    }
  }

  private void createRoot(String key, V value) {
    // Create new node
    try {
      root = new BTreeNode<V>(this.storageFolder, this.minimumDegree, true, new Pair<V>(key, value));
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    root.incrementKeyCount(); // Update number of keys in root
  }

  private void insertFull(String key, V value) {
    // Allocate memory for new root
    BTreeNode<V> s;
    try {
      s = new BTreeNode<V>(this.storageFolder, this.minimumDegree, false);
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
    if (s.getKey(i).key.compareTo(key) < 0)
      i++;
    s.getChildren().get(i).insertNonFull(key, value);

    // Change root
    root = s;
  }

  private boolean isRootFull() {
    return root.getKeyCount() == 2 * this.minimumDegree - 1;
  }
}

// A BTree node
class BTreeNode<V> {
  private int minimumDegree; // Minimum degree (defines the range for number of keys)
  private List<BTreeNode<V>> children; // An array of child pointers
  private int keyCount; // Current number of keys
  private boolean leaf; // Is true when node is leaf. Otherwise false
  private ChunkStorageHandler<V> storageInterface;
  private String storageFolder;

  // Constructor
  BTreeNode(String storageFolder, int t, boolean leaf, Pair<V> rootElement) throws Exception {
    this.minimumDegree = t;
    this.leaf = leaf;
    this.storageFolder = storageFolder;
    this.children = new ArrayList<BTreeNode<V>>(Collections.nCopies((2 * minimumDegree), null));
    this.keyCount = 0;
    String filePath = storageFolder + "/" + Integer.toString(this.hashCode());
    this.storageInterface = new ChunkStorageHandler<>(filePath);

    DatabaseChunk<V> newChunk = rootElement == null ? new DatabaseChunk<V>(minimumDegree) : new DatabaseChunk<V>(minimumDegree, Arrays.asList(rootElement));
    this.setChunk(newChunk);
  }

  BTreeNode(String storageFolder, int t, boolean leaf) throws Exception {
    this(storageFolder, t, leaf, null);
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

      Chunk<V> chunk = this.getChunk();

      if(chunk == null) {
        return;
      }

      Pair<V> pair = chunk.get(i);
      chunk = null;
      System.out.println(pair.key + " -> " + pair.value);
    }

    // Print the subtree rooted with last child
    if (leaf == false)
      this.children.get(i).traverse();
  }

  // A function to search a key in the subtree rooted with this node.
  V search(String k) { // returns NULL if k is not present.
    // Find the first key greater than or equal to k
    // int i = chunk.findIndexOfFirstGreaterThen(k);

    Chunk<V> chunk = this.getChunk();

    if (chunk == null) {
      return null;
    }

    int i = chunk.findIndexOfFirstGreaterThen(k);
    Pair<V> value = chunk.get(i);
    chunk = null;

    // If the found key is equal to k, return this node
    if (value.key.equals(k))
      return value.value;

    // If the key is not found here and this is a leaf node
    if (leaf == true)
      return null;

    // Go to the appropriate child
    return children.get(i).search(k);
  }

  // A utility function to insert a new key in this node
  // The assumption is, the node must be non-full when this
  // function is called
  void insertNonFull(String key, V value) {
    // Initialize index as index of rightmost element
    int i = keyCount - 1;

    Chunk<V> chunk = this.getChunk();

    // If this is a leaf node
    if (leaf == true) {
      // The following loop does two things
      // a) Finds the location of new key to be inserted
      // b) Moves all greater keys to one place ahead

      i = chunk.shiftRightOneAfterFirstGreaterThan(key);

      // Insert the new key at found location
      chunk.set(i + 1, new Pair<V>(key, value));

      this.setChunk(chunk);

      chunk = null;
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
      chunk = null;
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
      newNode = new BTreeNode<V>(this.storageFolder, child.minimumDegree, child.leaf);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    newNode.keyCount = this.minimumDegree - 1;

    Chunk<V> newNodeChunk = newNode.getChunk();
    Chunk<V> childChunk = child.getChunk();

    // Copy the last (t-1) keys of y to z
    for (int j = 0; j < this.minimumDegree - 1; j++) {
      newNodeChunk.set(j, childChunk.remove(j + this.minimumDegree));
    }

    newNode.setChunk(newNodeChunk);

    newNodeChunk = null;
    
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

    Chunk<V> chunk = this.getChunk();

    // A key of y will move to this node. Find the location of
    // new key and move all greater keys one space ahead
    chunk.shiftRightOne(i);

    // Copy the middle key of y to this node
    chunk.set(i, childChunk.remove(this.minimumDegree - 1));

    child.setChunk(childChunk);
    this.setChunk(chunk);;

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

  Pair<V> getKey(int i) {

    Chunk<V> chunk = this.getChunk();

    if (chunk == null) {
      return null;
    }

    return chunk.get(i);
  }

  Chunk<V> getChunk() {
    try {
      Chunk<V> chunk = storageInterface.readChunkFromMemory();
      return chunk;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  void setChunk(Chunk<V> chunk) {
    try {
      this.storageInterface.storeChunkInMemory(chunk);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    PersistentBtree<String> t = new PersistentBtree<String>(3, "database"); // A B-Tree with minimum degree 3
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
