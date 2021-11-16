package de.tum.i13.server.persistentStorage.btree;

import java.io.File;
import java.io.Serializable;

// A BTree
class PersistentBtree<V> implements Serializable {
  public BTreeNode<V> root; // Pointer to root node
  public int minimumDegree; // Minimum degree
  public String storageFolder;
  private TreeStorageHandler<V> storageHandler;
  public static int id = 0;

  private static final long serialVersionUID = 6529685098267757690L;

  // Constructor (Initializes tree as empty)
  PersistentBtree(int minimumDegree, String storageFolder, TreeStorageHandler<V> storageHandler) {
    this.root = null;
    this.minimumDegree = minimumDegree;
    this.storageFolder = storageFolder;
    this.storageHandler = storageHandler;
    this.createStorageFolder();
  }

  void remove(String key) {
    if (root == null)
      return;

    // Call the remove function for root
    root.remove(key);

    // If the root node has 0 keys, make its first child as the new root
    // if it has a child, otherwise set root as NULL
    if (root.getKeyCount() == 0) {
      root = root.isLeaf() ? null : root.getChildren().get(0);
    }

    return;
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
    if (root.isFull())
      this.insertFull(key, value);
    // If root is not full, call insertNonFull for root
    else 
      root.insertNonFull(key, value);

    this.storageHandler.saveToDisk(this);
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

    Chunk<V> chunk = s.getChunk();

    // Make old root as child of new root
    s.getChildren().set(0, root);

    // Split the old root and move 1 key to the new root
    s.splitChild(0, root, chunk);

    s.setChunk(chunk);

    // New root has two children now. Decide which of the
    // two children is going to have new key
    int i = 0;
    if (chunk.get(i).key.compareTo(key) < 0)
      i++;
    s.getChildren().get(i).insertNonFull(key, value);

    // Change root
    root = s;
  }

  /*
   * Display functions
   */

  // function to traverse the tree
  void traverse() {
    if (this.root != null)
      this.root.traverse();
    System.out.println();
  }

  void traverseCondensed() {
    if (this.root != null)
      this.root.traverseCondensed();
    System.out.println();
  }

  void traverseSpecial() {
    if (this.root != null)
      this.root.traverseSpecial();
    System.out.println();
  }

  public static void main(String[] args) {
    TreeStorageHandler<String> storageHandler = new TreeStorageHandler<String>("database", true);
    PersistentBtree<String> t = storageHandler.readFromDisk();// A B-Tree with minimum
                                                              // degree 3
    // t = t == null ? new PersistentBtree<String>(3, "database", storageHandler) :
    // t;
    // // t.insert("78", "a");
    // // t.insert("78", "b");
    // // t.insert("52", "b");
    // // t.insert("81", "c");
    // // t.insert("40", "d");
    // // t.insert("33", "e");
    // // t.insert("90", "f");
    // // t.insert("35", "g");
    // // t.insert("20", "h");
    // // t.insert("52", "c");
    // // t.insert("38", "h");

    // // BTree t(3); // A B-Tree with minimum degree 3

    // t.insert("A", "a");
    // t.traverseSpecial();

    // t.insert("C", "a");
    // t.insert("G", "a");
    // t.insert("J", "a");
    // t.insert("K", "a");
    // t.insert("M", "a");
    // t.insert("N", "a");
    // t.insert("O", "a");
    // t.traverseSpecial();

    // t.insert("R", "a");
    // t.insert("P", "a");
    // t.insert("S", "a");
    // t.insert("X", "a");
    // t.insert("Y", "a");
    // t.insert("Z", "a");
    // t.insert("U", "a");
    // t.insert("D", "a");
    // t.insert("E", "a");
    // t.insert("T", "a");
    // t.insert("V", "a");
    // t.insert("B", "a");
    // t.insert("Q", "a");
    // // t.traverseSpecial();
    // t.insert("L", "a");
    // // t.traverseSpecial();
    // t.insert("F", "a");
    // // t.traverseSpecial();

    // // t.traverseCondensed();

    // System.out.println("Traversal of tree constructed is");
    // t.traverseCondensed();

    // t.remove("F");
    // System.out.println("Traversal of tree after removing F");
    // t.traverseCondensed();
    // // t.traverseSpecial();

    // t.remove("M");
    // System.out.println("Traversal of tree after removing M");
    // t.traverseCondensed();

    // t.remove("G");
    // System.out.println("Traversal of tree after removing G");
    // t.traverseCondensed();

    // t.remove("D");
    // System.out.println("Traversal of tree after removing D");
    // t.traverseCondensed();

    // t.remove("B");
    // System.out.println("Traversal of tree after removing B");
    // t.traverseCondensed();

    // t.remove("P");
    // System.out.println("Traversal of tree after removing P");
    // t.traverseCondensed();

    // System.out.println("Traversal of the constructed tree is ");
    // t.traverse();

    String k = "E";

    if (t.search(k) != null) {
      System.out.println("\nPresent");
    } else {
      System.out.println("\nNot Present");
    }

    k = "F";
    if (t.search(k) != null)
      System.out.println("\nPresent");
    else
      System.out.println("\nNot Present");

    // t.insert("ZETOINO", "ABUTRE");

    t.traverseSpecial();
    t.traverseCondensed();
  }
}
