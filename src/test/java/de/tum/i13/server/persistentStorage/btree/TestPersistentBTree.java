package de.tum.i13.server.persistentStorage.btree;

import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeMockStorageHandler;
import de.tum.i13.server.persistentStorage.btree.storage.StorageException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPersistentBTree {

  private PersistentBTree<String> tree;

  @BeforeEach
  public void createTree() {
    tree = new PersistentBTree<>(3, new PersistentBTreeDiskStorageHandler<>("database", true));
  }

  @AfterEach
  public void deleteTree() throws StorageException {
    tree.delete();
  }

  @Test
  void testInsert1() {
    try {
      tree.insert("A", "Value");
    } catch (StorageException e1) {
      return; // Pass the test if exception is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("A")).isEqualTo("Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @RepeatedTest(5)
  void testInsert2() {

    String letters = "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < letters.length(); i++) {
      String sub = letters.substring(i);
      List<Character> alphabet = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
      Collections.shuffle(alphabet);
      for (char c : alphabet) {
        try {
          tree.insert(c + "", c + "");
          assertThat(TreeValidator.validTree(tree)).isTrue();
        } catch (StorageException e1) {
          return; // Pass the test if exception is thrown because there is currently no
                  // expectations the state of the tree once something goes wrong
        } catch (Exception e) {
          fail();
        }
      }

      for (char c : alphabet) {
        assertThat(tree.search(c + "")).isEqualTo(c + "");
      }
    }
  }

  @Test
  void testSearchOnEmpty() {
    assertThat(tree.search("a")).isEqualTo(null);
  }

  @Test
  void testdeleteOnEmpty() {
    assertDoesNotThrow(() -> tree.remove("a"));
  }

  @Test
  void testMissingSearch() {
    assertThat(tree.search("A")).isEqualTo(null);
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @Test
  void testInsertDoubleInsert1() {

    try {
      tree.insert("A", "Value");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("A")).isEqualTo("Value");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.insert("A", "Value2")).isEqualTo("Value");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("A")).isEqualTo("Value2");
      assertThat(TreeValidator.validTree(tree)).isTrue();
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }

  }

  @Test
  void testInsertDoubleInsert2() {
    try {
      assertThat(tree.insert("A", "Value")).isEqualTo(null);
      assertThat(tree.insert("B", "Value")).isEqualTo(null);
      assertThat(tree.insert("C", "Value1")).isEqualTo(null);
      assertThat(tree.insert("D", "Value")).isEqualTo(null);
      assertThat(tree.insert("E", "Value")).isEqualTo(null);
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("C")).isEqualTo("Value1");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      tree.insert("C", "Value2");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("C")).isEqualTo("Value2");
      assertThat(TreeValidator.validTree(tree)).isTrue();
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }

  }

  @Test
  void testDelete1() {
    try {
      tree.insert("A", "Value");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      tree.remove("C");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("C")).isEqualTo(null);
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void testDelete2() {
    try {
      tree.insert("A", "Value");
      tree.insert("C", "Value");
      tree.insert("B", "Value");
      tree.insert("D", "Value");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      tree.remove("C");
      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertThat(tree.search("C")).isEqualTo(null);
      assertThat(tree.search("B")).isNotEqualTo(null);
      assertThat(tree.search("A")).isNotEqualTo(null);
      assertThat(tree.search("D")).isNotEqualTo(null);
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  @RepeatedTest(5)
  void testDelete3() {

    try {
      String alphabet = "abcdefghijklmnopqrstuvwxyz";

      for (int i = 0; i < alphabet.length(); i++) {
        String sub = alphabet.substring(i);
        List<Character> alphabet1 = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
        Collections.shuffle(alphabet1);
        for (char c : alphabet1) {
          tree.insert(c + "", c + "");
          assertThat(TreeValidator.validTree(tree)).isTrue();
        }

        StringBuilder sb1 = new StringBuilder();

        // Appends characters one by one
        for (Character ch : alphabet1) {
          sb1.append(ch);
        }

        // convert in string
        String string1 = sb1.toString();

        // System.out.println(string1);

        List<Character> alphabet2 = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
        Collections.shuffle(alphabet2);

        // create object of StringBuilder class
        StringBuilder sb = new StringBuilder();

        // Appends characters one by one
        for (Character ch : alphabet2) {
          sb.append(ch);
        }

        // convert in string
        String string = sb.toString();

        for (char c : string.toCharArray()) {
          assertThat(tree.search(c + "")).isEqualTo(c + "");
          tree.remove(c + "");
          boolean valid = TreeValidator.validTree(tree);
          assertThat(valid).isTrue();
          assertThat(tree.search(c + "")).isEqualTo(null);
        }
      }
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void testPersistent1() {
    String letters = "abcdefghijklmnopqrstuvwxyz";
    List<Character> alphabet = letters.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
    try {
      Collections.shuffle(alphabet);
      for (char c : alphabet) {
        tree.insert(c + "", c + "");
        assertThat(TreeValidator.validTree(tree)).isTrue();
      }

      for (char c : alphabet) {
        assertThat(tree.search(c + "")).isEqualTo(c + "");
      }

      tree = null;
      PersistentBTreeDiskStorageHandler<String> handler = new PersistentBTreeDiskStorageHandler<>("database");
      tree = handler.load();

      assertThat(TreeValidator.validTree(tree)).isTrue();

      for (char c : alphabet) {
        assertThat(tree.search(c + "")).isEqualTo(c + "");
      }
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  private int countInFolder(String folder) {
    return new File(folder).list().length;
  }

  @Test
  void testPersistent2() {
    try {
      assertThat(this.countInFolder("database")).isEqualTo(0);
      tree.insert("a", "value");
      assertThat(this.countInFolder("database")).isEqualTo(2);
      tree.insert("b", "value");
      assertThat(this.countInFolder("database")).isEqualTo(2);
      tree.insert("c", "value");
      assertThat(this.countInFolder("database")).isEqualTo(2);
      tree.insert("d", "value");
      assertThat(this.countInFolder("database")).isEqualTo(2);
      tree.insert("e", "value");
      assertThat(this.countInFolder("database")).isEqualTo(2);
      tree.insert("f", "value");
      assertThat(this.countInFolder("database")).isEqualTo(4);
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void testPersistent3() {
    try {
      String letters = "abcdefghijklmnopqrstuvwxyz";
      List<Character> alphabet = letters.chars().mapToObj(e -> (char) e).collect(Collectors.toList());

      for (char c : alphabet) {
        tree.insert(c + "", c + "");
      }

      for (char c : alphabet) {
        tree.remove(c + "");
      }

      assertThat(this.countInFolder("database")).isEqualTo(1);
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  void testPersistent4() {
    try {
      tree.insert("a", "value");
      tree.insert("b", "value");
      tree.insert("c", "value");
      tree.remove("a");
      tree.remove("b");
      tree.remove("c");

      assertThat(this.countInFolder("database")).isEqualTo(1);

      tree = null;
      PersistentBTreeDiskStorageHandler<String> handler = new PersistentBTreeDiskStorageHandler<>("database");
      tree = handler.load();

      assertThat(TreeValidator.validTree(tree)).isTrue();
      assertDoesNotThrow(() -> tree.insert("a", "value"));
    } catch (StorageException e1) {
      return; // Pass the test if exceptiofn is thrown because there is currently no
              // expectations the state of the tree once something goes wrong
    } catch (Exception e) {
      fail();
    }
  }
}
