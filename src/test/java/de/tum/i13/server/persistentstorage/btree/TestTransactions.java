package de.tum.i13.server.persistentstorage.btree;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.tum.i13.server.persistentstorage.btree.io.PersistentBTreeDiskStorageHandler;
import de.tum.i13.server.persistentstorage.btree.io.StorageException;
import de.tum.i13.server.persistentstorage.btree.io.transactions.ChangeListenerImpl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TestTransactions {
    private PersistentBTree<String> tree;
    private static PersistentBTreeDisplay<String> display;
    private static PersistentBTreeDiskStorageHandler<String> sHandler;
    private static ChangeListenerImpl cHandler;

    @BeforeAll
    public static void setLogLevel() {
        Configurator.setRootLevel(Level.INFO);
        display = new PersistentBTreeDisplay<>();
    }

    @BeforeEach
    public void createTree() throws StorageException {
        cHandler = new ChangeListenerImpl("database");
        sHandler = new PersistentBTreeDiskStorageHandler<>("database", true, cHandler);
        tree = new PersistentBTree<>(3, sHandler, false);
    }

    @AfterEach
    public void deleteTree() throws StorageException {
        tree.delete();
    }

    private void doManualInsTransRolTransCycle(String key, String value)
            throws StorageException, PersistentBTreeException {
        String before, after;
        // Transaction 1
        before = display.traverseSpecial(tree);
        sHandler.beginTransaction();
        tree.insert(key, value);
        PersistentBTreeNode<String> newRoot = sHandler.rollbackTransaction();
        tree.setRoot(newRoot);
        after = display.traverseSpecial(tree);
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(after).isEqualTo(before);
        tree.insert(key, value);
        cHandler.reset();
    }

    private void doManualDelTransRolTransCycle(String key) throws StorageException, PersistentBTreeException {
        String before, after;
        // Transaction 1
        before = display.traverseSpecial(tree);
        sHandler.beginTransaction();
        tree.remove(key);
        PersistentBTreeNode<String> newRoot = sHandler.rollbackTransaction();
        tree.setRoot(newRoot);
        after = display.traverseSpecial(tree);
        assertThat(TreeValidator.validTree(tree)).isTrue();
        assertThat(after).isEqualTo(before);
        tree.remove(key);
        cHandler.reset();
    }

    @Test
    void rollbackOnInsertSimple() throws StorageException, PersistentBTreeException {
        this.doManualInsTransRolTransCycle("A", "A");
        this.doManualInsTransRolTransCycle("B", "B");
        this.doManualInsTransRolTransCycle("C", "C");
        this.doManualInsTransRolTransCycle("D", "D");
        this.doManualInsTransRolTransCycle("E", "E");
        this.doManualInsTransRolTransCycle("F", "F");
        this.doManualInsTransRolTransCycle("G", "A");
        this.doManualInsTransRolTransCycle("H", "A");
        this.doManualInsTransRolTransCycle("H", "A");
        this.doManualInsTransRolTransCycle("A", "A");
        this.doManualInsTransRolTransCycle("F", "A");
    }

    @Test
    void rollbackOnInsertStressTest() throws StorageException, PersistentBTreeException {

        String letters = "abcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < letters.length(); i++) {
            String sub = letters.substring(i);
            List<Character> alphabet = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
            Collections.shuffle(alphabet);
            for (char c : alphabet) {
                doManualInsTransRolTransCycle(c + "", c + "");
                assertThat(TreeValidator.validTree(tree)).isTrue();
            }
        }
    }

    @Test
    void rollbackOnDeleteSimple() throws StorageException, PersistentBTreeException {
        tree.insert("A", "A");
        tree.insert("B", "A");
        tree.insert("C", "A");
        tree.insert("D", "A");
        tree.insert("E", "A");
        tree.insert("F", "A");
        tree.insert("G", "A");
        tree.insert("H", "A");
        tree.insert("I", "A");
        tree.insert("J", "A");
        tree.insert("K", "A");
        tree.insert("L", "A");
        tree.insert("M", "A");
        tree.insert("N", "A");
        cHandler.reset();

        this.doManualDelTransRolTransCycle("A");
        this.doManualDelTransRolTransCycle("B");
        this.doManualDelTransRolTransCycle("C");
        this.doManualDelTransRolTransCycle("D");
        this.doManualDelTransRolTransCycle("E");
        this.doManualDelTransRolTransCycle("F");
        this.doManualDelTransRolTransCycle("G");
        this.doManualDelTransRolTransCycle("H");
        this.doManualDelTransRolTransCycle("I");
        this.doManualDelTransRolTransCycle("J");
        this.doManualDelTransRolTransCycle("K");
        this.doManualDelTransRolTransCycle("L");
        this.doManualDelTransRolTransCycle("M");
        this.doManualDelTransRolTransCycle("N");
    }

    @Test
    void rollbackOnDeleteComplex() throws StorageException, PersistentBTreeException {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < alphabet.length(); i++) {
            String sub = alphabet.substring(i);
            List<Character> alphabet1 = sub.chars().mapToObj(e -> (char) e).collect(Collectors.toList());
            Collections.shuffle(alphabet1);
            for (char c : alphabet1) {
                tree.insert(c + "", c + "");
                assertThat(TreeValidator.validTree(tree)).isTrue();
            }
            cHandler.reset();

            StringBuilder sb1 = new StringBuilder();

            // Appends characters one by one
            for (Character ch : alphabet1) {
                sb1.append(ch);
            }

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
                this.doManualDelTransRolTransCycle(c + "");
            }
        }
    }
}
