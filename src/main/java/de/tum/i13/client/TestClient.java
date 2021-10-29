package de.tum.i13.client;
import java.util.logging.Logger;


import static de.tum.i13.shared.LogSetup.setupLogging;

public class TestClient {


    private final static Logger LOGGER = Logger.getLogger(TestClient.class.getName());

    public static void main(String[] args) {
        setupLogging("test.log");

        try {
            EchoClient client = new EchoClient();

            client.connect("clouddatabases.msrg.in.tum.de", 5551);
            String tosend = "hello echo\r\n";
            client.send(tosend.getBytes());
            byte[] res = client.receive();
            System.out.println(new String (res, 0 , res.length - 2));
        } catch (Exception e) {
            LOGGER.throwing(TestClient.class.getName(), "main", e);
        }
    }
}
