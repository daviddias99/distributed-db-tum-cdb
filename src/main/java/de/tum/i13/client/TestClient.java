package de.tum.i13.client;

import java.util.logging.Logger;

import static de.tum.i13.shared.LogSetup.setupLogging;

public class TestClient {

    private final static Logger LOGGER = Logger.getLogger(TestClient.class.getName());

    public static void main(String[] args) {
        setupLogging("test.log");


        LOGGER.info("Creating a new Socket");
        //TODO

        try {
            LOGGER.info("Connecting to server");
            //TODO

            LOGGER.info("Getting the outputstream and inputstream");
            //TODO

            LOGGER.info("sending hello echo");
            //String tosend = "hello echo\r\n";
            //output.write(tosend);
            //output.flush(); //never forget to flush

            LOGGER.info("printing what the server has returned");
            //System.out.println(input.readLine());

        } catch (Exception e) {
            LOGGER.throwing(TestClient.class.getName(), "main", e);
        }
    }
}
