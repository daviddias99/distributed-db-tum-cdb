package de.tum.i13.client;

import java.util.logging.*;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import de.tum.i13.client.exceptions.ClientException;

public class Shell{    

    private static EchoClient client;
    //private static final Logger log = LogManager.getLogManager().getLogger("shell_logger");

    public Shell(){
    }

    public static void main(String[] args) throws IOException   {

        BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
        boolean quit = false;

        while (!quit) {
            //print prompt 
            System.out.print("EchoClient> ");

            //read user input from console
            String input = cons.readLine();
            String[] tokens = input.trim().split("\\s+");

            try {
                if( tokens.length == 3 && tokens[0].equals("connect"))
                    connect(tokens);
                else if( tokens.length == 1 && tokens[0].equals("disconnect"))
                    disconnect();
                else if( tokens.length == 2 && tokens[0].equals("send"))
                    send( tokens[1].getBytes());
                else if( tokens.length == 1 && tokens[0].equals("help"))
                    printHelp();
                else if( tokens.length == 1 && tokens[0].equals("quit")){
                    //TODO Disconnect existing connection?
                    disconnect();
                    quit = true;
                    System.out.println("Application shutting down");
                }
                else{
                    printUnknownCommand();
                }
            } catch (ClientException e) {
                System.out.println(e.getReason());
            }
            
        }
    }

    public static void connect( String[] tokens) throws ClientException{
        //TODO check if connected before??
        try {
            int port = Integer.parseInt(tokens[2]);
            String address = tokens[1];

            //create new connection and receive confirmation from server
            System.out.println("Connecting");
            client = new EchoClient(address, port);
            System.out.println("Connected");
            String confirmation = Arrays.toString(client.receive());
            System.out.println("server> " + confirmation);

        } catch (NumberFormatException e) {
            printUnknownCommand();
        }
    }

    public static void disconnect() throws ClientException{
        client.disconnect();
        System.out.println("Successfully disconnected");
    }

    public static void send( byte[] message) throws ClientException{
        client.send(message);
        String response = Arrays.toString(client.receive());

        System.out.println("server> " + response);
    }

    public static void printUnknownCommand(){
        System.out.println("Unknown command.");
        printHelp();
    }

    public static void printHelp(){
        //TODO
        System.out.println("help");
    }

}