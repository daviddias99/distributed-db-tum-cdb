package de.tum.i13;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSomething { //Classname has to start with Test


    @BeforeEach //gets called before the class initializes, perfect place to connect to a server
    public void initialize() {

    }

    @AfterEach //tearing down, perfect place to properly disconnect from a server or close a socket
    public void teardown() {

    }

    @Test //each method with Test annotation gets called
    public void testFramework() {
        assertEquals(true, true, "We can't trust Java");
    }
}
