package de.tum.i13.server.persistentstorage.btree;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class TestPersistentBTree {

    private PersistentBTree<String> tree;

    @BeforeEach
    public void createTree() throws StorageException {
        tree = new PersistentBTree<>(3, new PersistentBTreeDiskStorageHandler<>("database", true));
    }

    @AfterEach
    public void deleteTree() throws StorageException {
        tree.delete();
    }

    @Test
    void insertSingleValue() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("A")).isEqualTo("Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    @RepeatedTest(5)
    void insertMultipleValues() throws StorageException, PersistentBTreeException {

        String letters = "abcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < letters.length(); i++) {
            String sub = letters.substring(i);
            List<Character> alphabet = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
            Collections.shuffle(alphabet);
            for (char c : alphabet) {
                tree.insert(c + "", c + "");
                assertThat(TreeValidator.validTree(tree)).isTrue();

            }

            for (char c : alphabet) {
                assertThat(tree.search(c + "")).isEqualTo(c + "");
            }
        }
    }

    @Test
    void searchOnEmpty() throws StorageException, PersistentBTreeException {
        assertThat(tree.search("a")).isNull();
    }

    @Test
    void testdeleteOnEmpty() {
        assertDoesNotThrow(() -> tree.remove("a"));
    }

    @Test
    void missingValue() throws StorageException, PersistentBTreeException {
        assertThat(tree.search("A")).isNull();
        assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    @Test
    void insertAgainOneNode() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("A")).isEqualTo("Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.insert("A", "Value2")).isEqualTo("Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("A")).isEqualTo("Value2");
        assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    @Test
    void insertAgainTwoNodes() throws StorageException, PersistentBTreeException {
        assertThat(tree.insert("A", "Value")).isNull();
        assertThat(tree.insert("B", "Value")).isNull();
        assertThat(tree.insert("C", "Value1")).isNull();
        assertThat(tree.insert("D", "Value")).isNull();
        assertThat(tree.insert("E", "Value")).isNull();
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("C")).isEqualTo("Value1");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        tree.insert("C", "Value2");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("C")).isEqualTo("Value2");
        assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    @Test
    void deleteNonExistentOneNode() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        tree.remove("C");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("C")).isNull();
    }

    @Test
    void deleteSingleValue() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        tree.insert("C", "Value");
        tree.insert("B", "Value");
        tree.insert("D", "Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        tree.remove("C");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.search("C")).isNull();
        assertThat(tree.search("B")).isNotNull();
        assertThat(tree.search("A")).isNotNull();
        assertThat(tree.search("D")).isNotNull();
    }

    @Test
    void deleteNonExistentTwoNodes() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        tree.insert("C", "Value");
        tree.insert("B", "Value");
        tree.insert("D", "Value");
        tree.insert("E", "Value");
        tree.insert("F", "Value");
        tree.insert("G", "Value");
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(tree.remove("K")).isFalse();
        assertThat(TreeValidator.validTree(tree)).isTrue();
    }

    @RepeatedTest(5)
    void deleteMultipleValues() throws StorageException, PersistentBTreeException {

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
                assertThat(tree.remove(c + "")).isTrue();
                boolean valid = TreeValidator.validTree(tree);
                assertThat(valid).isTrue();
                assertThat(tree.search(c + "")).isNull();
            }
        }

    }

    @Test
    void persistsData() throws StorageException, PersistentBTreeException {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        List<Character> alphabet = letters.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
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
        tree = new PersistentBTree<>(3, handler.load(), handler);

        assertThat(TreeValidator.validTree(tree)).isTrue();

        for (char c : alphabet) {
            assertThat(tree.search(c + "")).isEqualTo(c + "");
        }
    }

    private int countInFolder(String folder) {
        return new File(folder).list().length;
    }

    @Test
    void createsDiskFiles() throws StorageException, PersistentBTreeException {
        assertThat(this.countInFolder("database")).isEqualTo(1);
        tree.insert("a", "value");
        assertThat(this.countInFolder("database")).isEqualTo(3);
        tree.insert("b", "value");
        assertThat(this.countInFolder("database")).isEqualTo(3);
        tree.insert("c", "value");
        assertThat(this.countInFolder("database")).isEqualTo(3);
        tree.insert("d", "value");
        assertThat(this.countInFolder("database")).isEqualTo(3);
        tree.insert("e", "value");
        assertThat(this.countInFolder("database")).isEqualTo(3);
        tree.insert("f", "value");
        assertThat(this.countInFolder("database")).isEqualTo(5);
    }

    @Test
    void deletesDiskFiles() throws StorageException, PersistentBTreeException {

        String letters = "abcdefghijklmnopqrstuvwxyz";
        List<Character> alphabet = letters.chars().mapToObj(e -> (char) e).collect(Collectors.toList());

        for (char c : alphabet) {
            tree.insert(c + "", c + "");
        }

        for (char c : alphabet) {
            tree.remove(c + "");
        }

        assertThat(this.countInFolder("database")).isEqualTo(2);
    }

    @Test
    void loadsCorrectly() throws StorageException, PersistentBTreeException {
        tree.insert("a", "value");
        tree.insert("b", "value");
        tree.insert("c", "value");
        tree.remove("a");
        tree.remove("b");
        tree.remove("c");

        assertThat(this.countInFolder("database")).isEqualTo(2);

        tree = null;
        PersistentBTreeDiskStorageHandler<String> handler = new PersistentBTreeDiskStorageHandler<>("database");
        tree = new PersistentBTree<>(3, handler.load(), handler);

        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertDoesNotThrow(() -> tree.insert("a", "value"));
    }

    @Test
    void getRange() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        tree.insert("C", "Value");
        tree.insert("B", "Value");
        tree.insert("D", "Value");
        tree.insert("E", "Value");
        tree.insert("F", "Value");
        tree.insert("G", "Value");
        System.out.println((new PersistentBTreeDisplay<String>()).traverseSpecial(tree));
        List<String> result = tree.getInRange("C", "F");

        for (String string : result) {
            System.out.println(string);
        }
    }

    @Test
    void getRange2() throws StorageException, PersistentBTreeException {
        tree.insert("A", "Value");
        tree.insert("C", "Value");
        tree.insert("B", "Value");
        tree.insert("D", "Value");
        tree.insert("E", "Value");
        tree.insert("F", "Value");
        tree.insert("G", "Value");
        tree.insert("H", "Value");
        tree.insert("J", "Value");
        tree.insert("K", "Value");
        tree.insert("L", "Value");
        tree.insert("M", "Value");
        tree.insert("N", "Value");
        tree.insert("O", "Value");
        System.out.println((new PersistentBTreeDisplay<String>()).traverseSpecial(tree));
        List<String> result = tree.getInRange("D", "K");

        for (String string : result) {
            System.out.println(string);
        }
    }
}
