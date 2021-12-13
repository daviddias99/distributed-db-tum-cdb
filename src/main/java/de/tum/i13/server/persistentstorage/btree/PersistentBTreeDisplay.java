package de.tum.i13.server.persistentstorage.btree;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;

/**
 * Display functions
 */
public class PersistentBTreeDisplay<V> {

    /**
     * Used to traverse the tree
     * 
     * @param tree tree to traverse
     * @return string containing the tree traversal
     */
    String traverse(PersistentBTree<V> tree) {
        StringBuilder result = new StringBuilder();
        PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
        if (tree.root != null)
            nodeDisplay.traverse(tree.root, result);
        return result.toString();
    }

    /**
     * Used to traverse the tree, showing summary information
     * 
     * @param tree tree to traverse
     * @return string containing the tree traversal
     */
    String traverseCondensed(PersistentBTree<V> tree) {
        StringBuilder result = new StringBuilder();

        PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
        if (tree.root != null)
            nodeDisplay.traverseCondensed(tree.root, result);
        return result.toString();

    }

    /**
     * Used to traverse the tree, showing complete information
     * 
     * @param tree tree to traverse
     * @return string containing the tree traversal
     */
    String traverseSpecial(PersistentBTree<V> tree) {
        StringBuilder result = new StringBuilder();

        PersistentBTreeNodeDisplay nodeDisplay = new PersistentBTreeNodeDisplay();
        if (tree.root != null)
            nodeDisplay.traverseSpecial(tree.root, result);
        return result.toString();

    }

    private class PersistentBTreeNodeDisplay {
        // A function to traverse all nodes in a subtree rooted with this node
        void traverse(PersistentBTreeNode<V> node, StringBuilder result) {

            // There are n keys and n+1 children, traverse through n keys
            // and first n children
            int i = 0;
            for (i = 0; i < node.elementCount; i++) {

                // If this is not leaf, then before printing key[i],
                // traverse the subtree rooted with child C[i].
                if (!node.leaf) {
                    this.traverse(node.children.get(i), result);
                }

                Chunk<V> chunk;
                try {
                    chunk = node.getChunk();
                } catch (StorageException e) {
                    return;
                }

                if (chunk == null) {
                    return;
                }

                Pair<V> pair = chunk.get(i);
                chunk.releaseStoredElements();
                result.append(pair.key + " -> " + pair.value + "\n");
            }

            // Print the subtree rooted with last child
            if (!node.leaf)
                this.traverse(node.children.get(i), result);
        }

        // A function to traverse all nodes in a subtree rooted with this node
        private void traverseCondensed(PersistentBTreeNode<V> node, StringBuilder result) {

            // There are n keys and n+1 children, traverse through n keys
            // and first n children
            int i = 0;
            for (i = 0; i < node.elementCount; i++) {

                // If this is not leaf, then before printing key[i],
                // traverse the subtree rooted with child C[i].
                if (!node.leaf) {
                    this.traverseCondensed(node.children.get(i), result);
                }

                Chunk<V> chunk;
                try {
                    chunk = node.getChunk();
                } catch (StorageException e) {
                    return;
                }

                if (chunk == null) {
                    return;
                }

                Pair<V> pair = chunk.get(i);
                chunk.releaseStoredElements();
                result.append(pair.key + "\n");
            }

            // Print the subtree rooted with last child
            if (!node.leaf)
                this.traverseCondensed(node.children.get(i), result);
        }

        void traverseSpecial(PersistentBTreeNode<V> node, StringBuilder result) {

            result.append("--\n");
            result.append("Node(" + Integer.toString(node.id) + ")\n");
            result.append("Key count: " + node.elementCount + "\n");
            result.append("Keys: ");

            // There are n keys and n+1 children, traverse through n keys
            // and first n children
            int i = 0;
            Chunk<V> chunk;
            try {
                chunk = node.getChunk();
            } catch (StorageException e) {
                return;
            }
            for (i = 0; i < node.elementCount; i++) {
                if (chunk == null) {
                    break;
                }

                Pair<V> pair = chunk.get(i);
                result.append(pair.key + " ");
            }

            if (chunk != null) {
                chunk.releaseStoredElements();
            }

            result.append("\n");

            result.append("Children: ");

            for (PersistentBTreeNode<V> bTreeNode : node.children) {

                if (bTreeNode == null) {
                    break;
                }

                result.append(Integer.toString(bTreeNode.id) + " ");
            }

            result.append("\n");

            for (PersistentBTreeNode<V> bTreeNode : node.children) {

                if (bTreeNode == null) {
                    break;
                }

                this.traverseSpecial(bTreeNode, result);
            }
        }

    }

}
