package de.tum.i13.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Logger;


import static de.tum.i13.shared.LogSetup.setupLogging;

public class TestClient {


    private final static Logger LOGGER = Logger.getLogger(TestClient.class.getName());

    public static void main(String[] args) {
        setupLogging("test.log");

        try {
            EchoClient client = new EchoClient();

            client.connect("clouddatabases.msrg.in.tum.de", 5551, true);
            String tosend = "hello echo\r\n";
            client.send(tosend.getBytes());
            byte[] res = client.receive();
            System.out.println(new String (res, 0 , res.length - 2));

            // LOGGER.info("Connecting to server");
            // Socket socket = new Socket("clouddatabases.msrg.in.tum.de", 5551);
            // LOGGER.info("Getting the outputstream and inputstream");

            // InputStream istream = socket.getInputStream();
            // OutputStream ostream = socket.getOutputStream();
            // byte[] receivedBytes = new byte[1024];

            // LOGGER.info("sending hello echo");
            // String tosend = "hello echo\r\n";
            // int returnedBytes = istream.read(receivedBytes);
            // System.out.println(new String(receivedBytes));

            // ostream.write(tosend.getBytes());
            // ostream.flush();
            // //output.write(tosend);
            // //output.flush(); //never forget to flush

            // LOGGER.info("printing what the server has returned");
            // returnedBytes = istream.read(receivedBytes);
            // System.out.println(returnedBytes);
            // // System.out.println(new String(receivedBytes, 0, returnedBytes - 2));

        } catch (Exception e) {
            LOGGER.throwing(TestClient.class.getName(), "main", e);
        }
    }
}
