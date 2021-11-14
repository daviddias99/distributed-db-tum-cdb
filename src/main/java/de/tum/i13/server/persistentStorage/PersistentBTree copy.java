// package de.tum.i13.server.persistentStorage;

// // Java program to illustrate the sum of two numbers

// // A BTree
// class PersistentBtreeB {
//   // public BTreeNode root; // Pointer to root node
//   public int t; // Minimum degree

//   // Constructor (Initializes tree as empty)
//   PersistentBtreeB(int t) {
//     this.root = null;
//     this.t = t;
//   }

//   // function to traverse the tree
//   public void traverse() {
//     if (this.root != null)
//       this.root.traverse();
//     System.out.println();
//   }

//   // function to search a key in this tree
//   public BTreeNode search(int k) {
//     if (this.root == null)
//       return null;
//     else
//       return this.root.search(k);
//   }

//   // The main function that inserts a new key in this B-Tree
//   void insert(int k) {
//     // If tree is empty
//     if (root == null) {
//       // Allocate memory for root
//       root = new BTreeNode(t, true);
//       root.keys[0] = k; // Insert key
//       root.n = 1; // Update number of keys in root
//     } else // If tree is not empty
//     {
//       // If root is full, then tree grows in height
//       if (root.n == 2 * t - 1) {
//         // Allocate memory for new root
//         BTreeNode s = new BTreeNode(t, false);

//         // Make old root as child of new root
//         s.C[0] = root;

//         // Split the old root and move 1 key to the new root
//         s.splitChild(0, root);

//         // New root has two children now. Decide which of the
//         // two children is going to have new key
//         int i = 0;
//         if (s.keys[0] < k)
//           i++;
//         s.C[i].insertNonFull(k);

//         // Change root
//         root = s;
//       } else // If root is not full, call insertNonFull for root
//         root.insertNonFull(k);
//     }
//   }

// }

// // A BTree node
// class BTreeNode {
//   public int[] keys; // An array of keys
//   public int t; // Minimum degree (defines the range for number of keys)
//   public BTreeNode[] C; // An array of child pointers
//   public int n; // Current number of keys
//   public boolean leaf; // Is true when node is leaf. Otherwise false

//   // Constructor
//   BTreeNode(int t, boolean leaf) {
//     this.t = t;
//     this.leaf = leaf;
//     this.keys = new int[2 * t - 1];
//     this.C = new BTreeNode[2 * t];
//     this.n = 0;
//   }

//   // A function to traverse all nodes in a subtree rooted with this node
//   public void traverse() {

//     // There are n keys and n+1 children, traverse through n keys
//     // and first n children
//     int i = 0;
//     for (i = 0; i < this.n; i++) {

//       // If this is not leaf, then before printing key[i],
//       // traverse the subtree rooted with child C[i].
//       if (this.leaf == false) {
//         C[i].traverse();
//       }
//       System.out.print(keys[i] + " ");
//     }

//     // Print the subtree rooted with last child
//     if (leaf == false)
//       C[i].traverse();
//   }

//   // A function to search a key in the subtree rooted with this node.
//   BTreeNode search(int k) { // returns NULL if k is not present.

//     // Find the first key greater than or equal to k
//     int i = 0;
//     while (i < n && k > keys[i])
//       i++;

//     // If the found key is equal to k, return this node
//     if (keys[i] == k)
//       return this;

//     // If the key is not found here and this is a leaf node
//     if (leaf == true)
//       return null;

//     // Go to the appropriate child
//     return C[i].search(k);

//   }

//   // A utility function to insert a new key in this node
//   // The assumption is, the node must be non-full when this
//   // function is called
//   void insertNonFull(int k) {
//     // Initialize index as index of rightmost element
//     int i = n - 1;

//     // If this is a leaf node
//     if (leaf == true) {
//       // The following loop does two things
//       // a) Finds the location of new key to be inserted
//       // b) Moves all greater keys to one place ahead
//       while (i >= 0 && keys[i] > k) {
//         keys[i + 1] = keys[i];
//         i--;
//       }

//       // Insert the new key at found location
//       keys[i + 1] = k;
//       n = n + 1;
//     } else // If this node is not leaf
//     {
//       // Find the child which is going to have the new key
//       while (i >= 0 && keys[i] > k)
//         i--;

//       // See if the found child is full
//       if (C[i + 1].n == 2 * t - 1) {
//         // If the child is full, then split it
//         splitChild(i + 1, C[i + 1]);

//         // After split, the middle key of C[i] goes up and
//         // C[i] is splitted into two. See which of the two
//         // is going to have the new key
//         if (keys[i + 1] < k)
//           i++;
//       }
//       C[i + 1].insertNonFull(k);
//     }
//   }

//   // A utility function to split the child y of this node
//   // Note that y must be full when this function is called
//   void splitChild(int i, BTreeNode y) {
//     // Create a new node which is going to store (t-1) keys
//     // of y
//     BTreeNode z = new BTreeNode(y.t, y.leaf);
//     z.n = t - 1;

//     // Copy the last (t-1) keys of y to z
//     for (int j = 0; j < t - 1; j++)
//       z.keys[j] = y.keys[j + t];

//     // Copy the last t children of y to z
//     if (y.leaf == false) {
//       for (int j = 0; j < t; j++)
//         z.C[j] = y.C[j + t];
//     }

//     // Reduce the number of keys in y
//     y.n = t - 1;

//     // Since this node is going to have a new child,
//     // create space of new child
//     for (int j = n; j >= i + 1; j--)
//       C[j + 1] = C[j];

//     // Link the new child to this node
//     C[i + 1] = z;

//     // A key of y will move to this node. Find the location of
//     // new key and move all greater keys one space ahead
//     for (int j = n - 1; j >= i; j--)
//       keys[j + 1] = keys[j];

//     // Copy the middle key of y to this node
//     keys[i] = y.keys[t - 1];

//     // Increment count of keys in this node
//     n = n + 1;
//   }

//   public static void main(String[] args) {
//     PersistentBtree t = new PersistentBtree(3); // A B-Tree with minimum degree 3
//     t.insert(10);
//     t.insert(20);
//     t.insert(5);
//     t.insert(6);
//     t.insert(12);
//     t.insert(30);
//     t.insert(7);
//     t.insert(17);

//     System.out.println("Traversal of the constructed tree is ");
//     t.traverse();

//     int k = 6;

//     if (t.search(k) != null) {
//       System.out.println("\nPresent");
//     } else {
//       System.out.println("\nNot Present");
//     }

//     k = 15;
//     if (t.search(k) != null)
//       System.out.println("\nPresent");
//     else
//       System.out.println("\nNot Present");
//   }
// }
