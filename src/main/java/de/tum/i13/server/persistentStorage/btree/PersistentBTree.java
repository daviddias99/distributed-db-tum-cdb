package de.tum.i13.server.persistentStorage.btree;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    System.out.println("Tree Structure");

    if (this.root != null)
      this.root.traverseSpecial();
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

  public void remove(String key) {
    if (root == null) {
      return;
    }

    // Call the remove function for root
    root.remove(key);

    // If the root node has 0 keys, make its first child as the new root
    // if it has a child, otherwise set root as NULL
    if (root.getKeyCount() == 0) {
      BTreeNode<V> tmp = root;
      if (root.isLeaf())
        root = null;
      else
        root = root.getChildren().get(0);

      // Free the old root
      tmp = null; // TODO: this might not be needed
    }
    return;
  }
}

// A BTree node
class BTreeNode<V> implements Serializable {
  private int minimumDegree; // Minimum degree (defines the range for number of keys)
  private List<BTreeNode<V>> children; // An array of child pointers
  private int keyCount; // Current number of keys
  private boolean leaf; // Is true when node is leaf. Otherwise false
  private ChunkStorageMock<V> storageInterface;
  private String storageFolder;
  private int id;

  // Constructor
  BTreeNode(String storageFolder, int t, boolean leaf, Pair<V> rootElement) throws Exception {
    this.minimumDegree = t;
    this.id = PersistentBtree.id++;
    this.leaf = leaf;
    this.storageFolder = storageFolder;
    this.children = new ArrayList<BTreeNode<V>>(Collections.nCopies((2 * minimumDegree), null));
    this.keyCount = 0;
    String filePath = storageFolder + "/" + Integer.toString(this.id);
    this.storageInterface = new ChunkStorageMock<>(filePath);

    DatabaseChunk<V> newChunk = rootElement == null ? new DatabaseChunk<V>(minimumDegree)
        : new DatabaseChunk<V>(minimumDegree, Arrays.asList(rootElement));
    this.setChunk(newChunk);
  }

  BTreeNode(String storageFolder, int t, boolean leaf) throws Exception {
    this(storageFolder, t, leaf, null);
  }

  boolean isLeaf() {
    return this.leaf;
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

      if (chunk == null) {
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

    // A function to traverse all nodes in a subtree rooted with this node
    void traverseCondensed() {

      // There are n keys and n+1 children, traverse through n keys
      // and first n children
      int i = 0;
      for (i = 0; i < this.keyCount; i++) {
  
        // If this is not leaf, then before printing key[i],
        // traverse the subtree rooted with child C[i].
        if (this.leaf == false) {
          this.children.get(i).traverseCondensed();
        }
  
        Chunk<V> chunk = this.getChunk();
  
        if (chunk == null) {
          return;
        }
  
        Pair<V> pair = chunk.get(i);
        chunk = null;
        System.out.print(pair.key + " ");
      }
  
      // Print the subtree rooted with last child
      if (leaf == false)
        this.children.get(i).traverseCondensed();
    }

  void traverseSpecial() {

    System.out.println("--");
    System.out.println("Node(" + Integer.toString(this.id) + ")");
    System.out.println("Key count: " + this.keyCount);
    System.out.print("Keys: ");
    
    // There are n keys and n+1 children, traverse through n keys
    // and first n children
    int i = 0;
    Chunk<V> chunk = this.getChunk();
    for (i = 0; i < this.keyCount; i++) {
      if (chunk == null) {
        break;
      }

      Pair<V> pair = chunk.get(i);
      System.out.print(pair.key + " " );
    }
    chunk = null;
    System.out.print("\n");
    
    System.out.print("Children: ");
    
    for (BTreeNode<V> bTreeNode : children) {

      if (bTreeNode == null) {
        break;
      }

      System.out.print(Integer.toString(bTreeNode.id) + " ");
    }

    System.out.print("\n");

    for (BTreeNode<V> bTreeNode : children) {

      if (bTreeNode == null) {
        break;
      }

      bTreeNode.traverseSpecial();
    }
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

      i = chunk.shiftRightOneAfterFirstGreaterThan(key, this.keyCount);

      // Insert the new key at found location
      chunk.set(i + 1, new Pair<V>(key, value));

      this.setChunk(chunk);
      this.incrementKeyCount();
      chunk = null;
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
        child.children.set(j + this.minimumDegree, null);
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
    chunk.shiftRightOne(i, this.keyCount);

    // Copy the middle key of y to this node
    chunk.set(i, childChunk.remove(this.minimumDegree - 1));

    child.setChunk(childChunk);
    this.setChunk(chunk);

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

  int decrementKeyCount() {
    this.keyCount--;
    return this.keyCount;
  }

  int setKeyCount(int newKeyCount) {
    this.keyCount = newKeyCount;
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

  // A utility function that returns the index of the first key that is
  // greater than or equal to k
  int findKey(String key) {

    Chunk<V> chunk = this.getChunk();

    int idx = 0;
    while (idx < this.keyCount && chunk.get(idx).key.compareTo(key) < 0)
      ++idx;
    return idx;
  }

  // A function to remove the key k from the sub-tree rooted with this node
  void remove(String key) {
    int idx = findKey(key);

    Chunk<V> chunk = this.getChunk();

    // The key to be removed is present in this node
    if (idx < this.keyCount && chunk.get(idx).key.compareTo(key) == 0) {

      // If the node is a leaf node - removeFromLeaf is called
      // Otherwise, removeFromNonLeaf function is called
      if (leaf)
        removeFromLeaf(idx);
      else
        removeFromNonLeaf(idx);
    } else {

      // If this node is a leaf node, then the key is not present in tree
      if (leaf) {
        return;
      }

      // The key to be removed is present in the sub-tree rooted with this node
      // The flag indicates whether the key is present in the sub-tree rooted
      // with the last child of this node
      boolean flag = ((idx == this.keyCount) ? true : false);

      List<BTreeNode<V>> children = this.getChildren();

      // If the child where the key is supposed to exist has less that t keys,
      // we fill that child
      if (children.get(idx).getKeyCount() < this.minimumDegree)
        fill(idx);

      // If the last child has been merged, it must have merged with the previous
      // child and so we recurse on the (idx-1)th child. Else, we recurse on the
      // (idx)th child which now has atleast t keys
      if (flag && idx > this.keyCount)
        children.get(idx - 1).remove(key);
      else
        children.get(idx).remove(key);
    }
    return;
  }

  // A function to remove the idx-th key from this node - which is a leaf node
  void removeFromLeaf(int idx) {

    Chunk<V> chunk = this.getChunk();

    // Move all the keys after the idx-th pos one place backward
    chunk.shiftLeftOne(idx, keyCount);

    this.setChunk(chunk);

    // Reduce the count of keys
    this.keyCount--;

    return;
  }

  // A function to remove the idx-th key from this node - which is a non-leaf node
  void removeFromNonLeaf(int idx) {

    Chunk<V> chunk = this.getChunk();

    Pair<V> k = chunk.get(idx);

    // If the child that precedes k (C[idx]) has atleast t keys,
    // find the predecessor 'pred' of k in the subtree rooted at
    // C[idx]. Replace k by pred. Recursively delete pred
    // in C[idx]
    if (this.getChildren().get(idx).getKeyCount() >= this.minimumDegree) {
      Pair<V> pred = getPred(idx);
      chunk.set(idx, pred);
      this.getChildren().get(idx).remove(pred.key);
    }

    // If the child C[idx] has less that t keys, examine C[idx+1].
    // If C[idx+1] has atleast t keys, find the successor 'succ' of k in
    // the subtree rooted at C[idx+1]
    // Replace k by succ
    // Recursively delete succ in C[idx+1]
    else if (this.getChildren().get(idx + 1).getKeyCount() >= this.minimumDegree) {
      Pair<V> succ = getSucc(idx);
      chunk.set(idx, succ);
      this.getChildren().get(idx + 1).remove(succ.key);
    }

    // If both C[idx] and C[idx+1] has less that t keys,merge k and all of C[idx+1]
    // into C[idx]
    // Now C[idx] contains 2t-1 keys
    // Free C[idx+1] and recursively delete k from C[idx]
    else {
      merge(idx);
      this.getChildren().get(idx).remove(k.key);
    }

    this.setChunk(chunk);

    return;
  }

  // A function to get predecessor of keys[idx]
  Pair<V> getPred(int idx) {
    // Keep moving to the right most node until we reach a leaf
    BTreeNode<V> cur = this.getChildren().get(idx);
    Chunk<V> chunk = cur.getChunk();

    while (!cur.isLeaf())
      cur = cur.getChildren().get(cur.getKeyCount());

    // Return the last key of the leaf
    return chunk.get(cur.getKeyCount() - 1);
  }

  Pair<V> getSucc(int idx) {

    // Keep moving the left most node starting from C[idx+1] until we reach a leaf
    BTreeNode<V> cur = this.getChildren().get(idx + 1);
    Chunk<V> chunk = cur.getChunk();

    while (!cur.isLeaf())
      cur = cur.getChildren().get(0);

    // Return the first key of the leaf
    return chunk.get(0);
  }

  // A function to fill child C[idx] which has less than t-1 keys
  void fill(int idx) {

    // If the previous child(C[idx-1]) has more than t-1 keys, borrow a key
    // from that child
    if (idx != 0 && this.getChildren().get(idx - 1).getKeyCount() >= this.minimumDegree)
      borrowFromPrev(idx);

    // If the next child(C[idx+1]) has more than t-1 keys, borrow a key
    // from that child
    else if (idx != this.getKeyCount() && this.getChildren().get(idx + 1).getKeyCount() >= this.minimumDegree)
      borrowFromNext(idx);

    // Merge C[idx] with its sibling
    // If C[idx] is the last child, merge it with with its previous sibling
    // Otherwise merge it with its next sibling
    else {
      if (idx != this.getKeyCount())
        merge(idx);
      else
        merge(idx - 1);
    }
    return;
  }

  // A function to borrow a key from C[idx-1] and insert it
  // into C[idx]
  void borrowFromPrev(int idx) {

    BTreeNode<V> child = this.getChildren().get(idx);
    BTreeNode<V> sibling = this.getChildren().get(idx - 1);

    // The last key from C[idx-1] goes up to the parent and key[idx-1]
    // from parent is inserted as the first key in C[idx]. Thus, the loses
    // sibling one key and child gains one key

    Chunk<V> childChunk = child.getChunk();

    // Moving all key in C[idx] one step ahead
    for (int i=child.getKeyCount()-1; i>=0; --i) {
      childChunk.set(i + 1, childChunk.get(i));
    }

    Chunk<V> chunk = this.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // If C[idx] is not a leaf, move all its child pointers one step ahead
    if (!child.isLeaf()) {
      for (int i = child.getKeyCount(); i >= 0; --i)
        child.getChildren().set(i + 1, child.getChildren().get(i));
    }

    // Setting child's first key equal to keys[idx-1] from the current node
    childChunk.set(0, chunk.get(idx - 1));

    // Moving sibling's last child as C[idx]'s first child
    if (!child.isLeaf()) {
      child.getChildren().set(0, sibling.getChildren().get(sibling.keyCount));
    }

    // Moving the key from the sibling to the parent
    // This reduces the number of keys in the sibling
    chunk.set(idx - 1, siblingChunk.get(sibling.keyCount - 1));

    child.incrementKeyCount();
    sibling.decrementKeyCount();

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);

    return;
  }

  // A function to borrow a key from the C[idx+1] and place
  // it in C[idx]
  void borrowFromNext(int idx) {

    BTreeNode<V> child = this.children.get(idx);
    BTreeNode<V> sibling = this.children.get(idx + 1);

    Chunk<V> chunk = this.getChunk();
    Chunk<V> childChunk = child.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // keys[idx] is inserted as the last key in C[idx]
    childChunk.set(child.getKeyCount(), chunk.get(idx));

    // Sibling's first child is inserted as the last child
    // into C[idx]
    if (!(child.isLeaf()))
      child.getChildren().set(child.getKeyCount() + 1, sibling.getChildren().get(0));

    // The first key from sibling is inserted into keys[idx]
    chunk.set(idx, siblingChunk.get(0));

    // Moving all keys in sibling one step behind
    for (int i = 1; i < sibling.keyCount; ++i) {
      siblingChunk.set(i - 1, siblingChunk.get(i));
    }

    // Moving the child pointers one step behind
    if (!sibling.leaf) {
      for (int i = 1; i <= sibling.keyCount; ++i)
        sibling.getChildren().set(i - 1, sibling.getChildren().get(i));
    }

    // Increasing and decreasing the key count of C[idx] and C[idx+1]
    // respectively

    child.incrementKeyCount();
    sibling.decrementKeyCount();

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);

    return;
  }

  // A function to merge C[idx] with C[idx+1]
  // C[idx+1] is freed after merging
  void merge(int idx) {
    BTreeNode<V> child = this.getChildren().get(idx);
    BTreeNode<V> sibling = this.getChildren().get(idx + 1);

    Chunk<V> chunk = this.getChunk();
    Chunk<V> childChunk = child.getChunk();
    Chunk<V> siblingChunk = sibling.getChunk();

    // Pulling a key from the current node and inserting it into (t-1)th
    // position of C[idx]
    childChunk.set(this.minimumDegree - 1, chunk.get(idx));

    // Copying the keys from C[idx+1] to C[idx] at the end
    for (int i = 0; i < sibling.getKeyCount(); ++i) {
      childChunk.set(i + this.minimumDegree, siblingChunk.get(i));
      siblingChunk.set(i, null);
    }

    // Copying the child pointers from C[idx+1] to C[idx]
    if (!child.isLeaf()) {
      for (int i = 0; i <= sibling.getKeyCount(); ++i) {
        child.getChildren().set(i + this.minimumDegree, sibling.getChildren().get(i));
      }
    }

    // Moving all keys after idx in the current node one step before -
    // to fill the gap created by moving keys[idx] to C[idx]
    for (int i = idx + 1; i < this.getKeyCount(); ++i) 
      chunk.set(i - 1, chunk.get(i));

    // Moving the child pointers after (idx+1) in the current node one
    // step before
    for (int i = idx + 2; i <= this.keyCount; ++i) 
      this.getChildren().set(i - 1, this.getChildren().get(i));

    // Updating the key count of child and the current node
    child.setKeyCount(child.getKeyCount() + sibling.getKeyCount() + 1);
    this.decrementKeyCount();

    // Freeing the memory occupied by sibling

    this.setChunk(chunk);
    child.setChunk(childChunk);
    sibling.setChunk(siblingChunk);
    return;
  }

  public static void main(String[] args) {
    TreeStorageHandler<String> storageHandler = new TreeStorageHandler<String>("database", true);
    PersistentBtree<String> t = storageHandler.readFromDisk();// A B-Tree with minimum
                                                              // degree 3
    t = t == null ? new PersistentBtree<String>(3, "database", storageHandler) : t;
    // t.insert("78", "a");
    // t.insert("78", "b");
    // t.insert("52", "b");
    // t.insert("81", "c");
    // t.insert("40", "d");
    // t.insert("33", "e");
    // t.insert("90", "f");
    // t.insert("35", "g");
    // t.insert("20", "h");
    // t.insert("52", "c");
    // t.insert("38", "h");

    // BTree t(3); // A B-Tree with minimum degree 3
  
    t.insert("A", "a");
    t.insert("C", "a");
    t.insert("G", "a");
    t.insert("J", "a");
    t.insert("K", "a");
    t.insert("M", "a");
    t.insert("N", "a");
    t.insert("O", "a");
    t.insert("R", "a");
    t.insert("P", "a");
    t.insert("S", "a");
    t.insert("X", "a");
    t.insert("Y", "a");
    t.insert("Z", "a");
    t.insert("U", "a");
    t.insert("D", "a");
    t.insert("E", "a");
    t.insert("T", "a");
    t.insert("V", "a");
    t.insert("B", "a");
    t.insert("Q", "a");
    // t.traverseSpecial();
    t.insert("L", "a");
    // t.traverseSpecial();
    t.insert("F", "a");
    // t.traverseSpecial();

    // t.traverseCondensed();

    System.out.println("Traversal of tree constructed is");
    t.traverseCondensed();
    
  
    t.remove("F");
    System.out.println("Traversal of tree after removing F");
    t.traverseCondensed();
    // t.traverseSpecial();
  
  
    t.remove("M");
    System.out.println("Traversal of tree after removing M");
    t.traverseCondensed();
    
  
    t.remove("G");
    System.out.println("Traversal of tree after removing G");
    t.traverseCondensed();
    
  
    t.remove("D");
    System.out.println("Traversal of tree after removing D");
    t.traverseCondensed();
    
  
    t.remove("B");
    System.out.println("Traversal of tree after removing B");
    t.traverseCondensed();
    
  
    t.remove("P");
    System.out.println("Traversal of tree after removing P");
    t.traverseCondensed();

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

    t.insert("ZETOINO", "ABUTRE");

    t.traverseSpecial();
    t.traverseCondensed();
  }
}
