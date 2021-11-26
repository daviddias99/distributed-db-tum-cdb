package de.tum.i13.server.persistentstorage.btree;

import java.util.Set;
import java.util.TreeSet;

import de.tum.i13.server.persistentstorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentstorage.btree.chunk.Pair;
import de.tum.i13.server.persistentstorage.btree.storage.StorageException;

import java.util.List;

public class TreeValidator {
    public static boolean validTree(PersistentBTree<String> tree) {

        Set<String> keys = new TreeSet<>();

        if (tree.root == null) {
            return true;
        }

        return validTreeRec(tree.root, tree.getMininumDegree(), keys);
    }

    public static boolean equalTrees(PersistentBTree<String> tree1, PersistentBTree<String> tree2) {

        if (tree1.root == null && tree2.root == null) {
            return true;
        }

        return equalTreesRec(tree1.root, tree2.root, tree1.getMininumDegree());
    }

    private static boolean equalTreesRec(PersistentBTreeNode<String> node1, PersistentBTreeNode<String> node2,
            int minimumDegree) {
        int keyCount1 = node1.getElementCount();
        int keyCount2 = node2.getElementCount();

        if (keyCount1 != keyCount2) {
            return false;
        }

        if (node1.getChildren().size() != node2.getChildren().size()) {
            return false;
        }

        Chunk<String> chunk1;
        Chunk<String> chunk2;
        try {
            chunk1 = node1.getChunk();
            chunk2 = node2.getChunk();
        } catch (StorageException e) {
            return false;
        }

        List<Pair<String>> chunkElements1 = chunk1.getElements();
        List<Pair<String>> chunkElements2 = chunk2.getElements();

        if (chunkElements1.size() != chunkElements2.size()) {
            return false;
        }

        for (int i = 0; i < chunkElements1.size(); i++) {
            if (chunkElements1.get(i).key != chunkElements2.get(i).key) {
                return false;
            }
            if (chunkElements1.get(i).value != chunkElements2.get(i).value) {
                return false;
            }
        }

        for (int i = 0; i < node1.getChildren().size(); i++) {
            if (node1.getChildren().get(i) == null && node2.getChildren().get(i) == null) {
                continue;
            }

            boolean res = equalTreesRec(node1.getChildren().get(i), node2.getChildren().get(i), minimumDegree);

            if (!res) {
                return false;
            }
        }

        return true;
    }

    private static boolean validTreeRec(PersistentBTreeNode<String> node, int minimumDegree, Set<String> keys) {
        int keyCount = node.getElementCount();
        Chunk<String> chunk;
        try {
            chunk = node.getChunk();
        } catch (StorageException e) {
            return false;
        }

        List<Pair<String>> chunkElements = chunk.getElements();

        for (Pair<String> pair : chunkElements) {
            if (pair == null)
                break;

            boolean success = keys.add(pair.key);

            if (!success) {
                return false;
            }
        }

        if (keyCount != chunk.getElementCount()) {
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
