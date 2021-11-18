package de.tum.i13.server.persistentStorage.btree;

import de.tum.i13.server.persistentStorage.btree.storage.PersistentBTreeMockStorageHandler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPersistentBTree {

  private PersistentBTree<String> tree;

  @BeforeEach
  public void createTree() {
    tree = new PersistentBTree<>(3, new PersistentBTreeMockStorageHandler<>());
  }

  @Test
  void testInsert1() {

    tree.insert("A", "Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("A")).isEqualTo("Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @RepeatedTest(100)
  void testInsert2() {
    List<Character> alphabet = "abcdefghijklmnopqrstuvwxyz".chars().mapToObj(e -> (char) e)
        .collect(Collectors.toList());
    Collections.shuffle(alphabet);
    for (char c : alphabet) {
      tree.insert(c + "", c + "");
      assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    for (char c : alphabet) {
      assertThat(tree.search(c + "")).isEqualTo(c + "");
    }
  }

  @Test
  void testMissingSearch() {
    assertThat(tree.search("A")).isEqualTo(null);
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @Test
  void testInsertDoubleInsert1() {

    tree.insert("A", "Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("A")).isEqualTo("Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    tree.insert("A", "Value2");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("A")).isEqualTo("Value2");
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @Test
  void testInsertDoubleInsert2() {

    tree.insert("A", "Value");
    tree.insert("B", "Value");
    tree.insert("C", "Value1");
    tree.insert("D", "Value");
    tree.insert("E", "Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("C")).isEqualTo("Value1");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    tree.insert("C", "Value2");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("C")).isEqualTo("Value2");
    assertThat(TreeValidator.validTree(tree)).isTrue();
  }

  @Test
  void testDelete1() {

    tree.insert("A", "Value");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    tree.remove("C");
    assertThat(TreeValidator.validTree(tree)).isTrue();
    assertThat(tree.search("C")).isEqualTo(null);
  }

  @Test
  void testDelete2() {
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
  }

  @Test
  void testDelete4() {
    List<Character> alphabet1 = "kgotjyandicpbxlshwzfuqemrv".chars().mapToObj(e -> (char) e)
        .collect(Collectors.toList());
    for (char c : alphabet1) {
      tree.insert(c + "", c + "");
    }
    // tree.traverseSpecial();
    //kunqyamxrwjbcflstedzvhipog
    tree.traverseSpecial();
    tree.remove("k");
    tree.remove("u");
    String find = tree.search("n");
    boolean valid = TreeValidator.validTree(tree);
    tree.remove("n");
    tree.remove("q");
    tree.remove("y");
    tree.remove("a");
    tree.remove("m");
    tree.remove("x");
    tree.remove("r");
    tree.traverseSpecial();
    tree.remove("w");
    tree.traverseSpecial();
    tree.remove("j");
    tree.remove("b");
    tree.remove("c");
    tree.remove("f");
    tree.remove("l");
    tree.traverseSpecial();
    tree.remove("s");
    // boolean valid = TreeValidator.validTree(tree);
    // if (!valid) {
    //   // tree.traverseSpecial();
    //   TreeValidator.validTree(tree);
    // }
  }

  @RepeatedTest(100)
  void testDelete3() {
    
    List<Character> alphabet1 = "abcdefghijklmnopqrstuvwxyz".chars().mapToObj(e -> (char) e)
        .collect(Collectors.toList());
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

    System.out.println(string1);

    List<Character> alphabet2 = "kunqyamxrwjbcflstedzvhipog".chars().mapToObj(e -> (char) e)
        .collect(Collectors.toList());
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
      
      String searchResult = tree.search(c + "");

      if(searchResult == null) {
        System.out.println("---->" + string1 + " - " + c);
      }

      assertThat(tree.search(c + "")).isEqualTo(c + "");
      tree.remove(c + "");
      boolean valid = TreeValidator.validTree(tree);

      if (!valid) {
        System.out.println(string);
        System.out.println(c);
        boolean b = TreeValidator.validTree(tree);
        tree.traverseSpecial();
      }

      assertThat(valid).isTrue();
      assertThat(tree.search(c + "")).isEqualTo(null);
    }
  }

}
