package de.tum.i13.server.persistentStorage.btree;

import de.tum.i13.server.persistentStorage.btree.chunk.Chunk;
import de.tum.i13.server.persistentStorage.btree.chunk.Pair;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeMockStorageHandler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

class TestPersistentBTree {

    private PersistentBTree<String> tree;

    private boolean validTree() {

      Set<String> keys = new TreeSet<>();

      if(tree.root == null) {
        return true;
      }

      return validTreeRec(tree.root, tree.minimumDegree, keys);
    }
  
    private boolean validTreeRec(PersistentBTreeNode<String> node, int minimumDegree, Set<String> keys) {
      int keyCount = node.getKeyCount();
      Chunk<String> chunk = node.getChunk();
      List<Pair<String>> chunkElements = chunk.getKeys();
      
      for (Pair<String> pair : chunkElements) {
        if(pair == null)
          break;
        
        boolean success = keys.add(pair.key);

        if(!success) {
          return false;
        }
      }


      if(keyCount != chunk.getKeyCount()) {
        return false;
      }

      if (node.getChildrenCount() > 2 * minimumDegree - 1) {
        return false;
      }

      for (PersistentBTreeNode<String> child : node.getChildren()) {

        if(child == null) {
          break;
        }

        boolean res = validTreeRec(child, minimumDegree, keys);

        if(!res) {
          return false;
        }
      }

      return true;
    }

    @BeforeEach
    public void createTree() {
      tree = new PersistentBTree<>(3, new PersistentBTreeMockStorageHandler<>());
    }
    
    @Test
    void testInsert1() {

      tree.insert("A", "Value");
      assertThat(validTree()).isTrue();
      assertThat(tree.search("A")).isEqualTo("Value");
      assertThat(validTree()).isTrue();
    }

    @Test
    void testMissingSearch() {
      assertThat(tree.search("A")).isEqualTo(null);
      assertThat(validTree()).isTrue();
    }

    @Test
    void testInsertDoubleInsert() {

      tree.insert("A", "Value");
      assertThat(validTree()).isTrue();
      assertThat(tree.search("A")).isEqualTo("Value");
      assertThat(validTree()).isTrue();
      tree.insert("A", "Value2");
      assertThat(validTree()).isTrue();
      assertThat(tree.search("A")).isEqualTo("Value2");
      assertThat(validTree()).isTrue();
    }
}
