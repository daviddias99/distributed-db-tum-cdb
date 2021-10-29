package de.tum.i13.client;

import org.junit.jupiter.api.*;
import org.mockito.internal.util.reflection.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;



import java.net.Socket;

public class TestEchoClient {

  private Socket mySocket;
  
  @BeforeEach //gets called before the class initializes, perfect place to connect to a server
  public void initialize() {
    mySocket = mock(Socket.class);
  }

  @AfterEach //tearing down, perfect place to properly disconnect from a server or close a socket
  public void teardown() {

  }

  @Test //each method with Test annotation gets called
  public void testFramework() {
      EchoClient client = new EchoClient();
      FieldSetter setter;
      assertEquals(true, true, "We can't trust Java");
  }
}
