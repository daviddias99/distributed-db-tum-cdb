package de.tum.i13.client;

import java.util.logging.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.io.IOException;

import de.tum.i13.client.exceptions.ClientException.ClientException;
import de.tum.i13.shared.Constants;

public class Shell{    

    private static EchoClient client = new EchoClient();
    private final static Logger LOGGER = Logger.getLogger(Shell.class.getName());

    private static String address;              //server address of current connection, for logging purposes
    private static int port;                    //port number of current connection

    /**
     * Creates a new command line interface.
     */
    public Shell(){
    }

    /**
     * Main method. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to <address>:<port> , disconnect, send a message to the server, change logging level. 
     */
    public static void main(String[] args) throws IOException   {
        //initialize logger level
        LOGGER.setLevel(Level.SEVERE);

        BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
        boolean quit = false;

        while (!quit) {
            //print prompt 
            System.out.print("EchoClient> ");

            //read user input from console
            String input = cons.readLine();
            String[] tokens = input.trim().split("\\s+");

            try {
                //connect command should be in format: "connect <address> <port>"
                if( tokens.length == 3 && tokens[0].equals("connect")){
                    connect(tokens);
                }
                //disconnect command should provide status report upon successful disconnection
                else if( tokens.length == 1 && tokens[0].equals("disconnect")){
                    LOGGER.info(String.format("Disconnecting from %s:%d", address, port));
                    client.disconnect();
                    System.out.println("EchoClient> Successfully disconnected.");
                }
                //command to send message to server: "send <message>"
                else if( tokens.length == 2 && tokens[0].equals("send")){
                    //send the message in bytes after appending the delimiter
                    LOGGER.info(String.format("Sending message to %s:%d", address, port));
                    client.send( (tokens[1] + "\r\n").getBytes());
                    receiveMessage();
                }
                //command to change the logging level: "logLevel <level>"
                else if( tokens.length == 2 && tokens[0].equals("logLevel")){
                    changeLogLevel(tokens[1]);
                }
                //help command to print information about the program
                else if( tokens.length == 1 && tokens[0].equals("help")){
                    printHelp();
                }
                //quit command should close any existing connection before quitting the program
                else if( tokens.length == 1 && tokens[0].equals("quit")){
                    if(client.isConnected()){
                        LOGGER.info(String.format("Disconnecting from %s:%d", address, port));
                        client.disconnect();
                    }

                    LOGGER.info("Quitting application.");
                    quit = true;
                    System.out.println("EchoClient> Application exit!");
                }
                //unrecognized input
                else{
                    LOGGER.info("Unrecognized command.");
                    printHelp();
                }
            } catch (ClientException e) {
                LOGGER.severe(String.format("Exception type: %s. Exception reason: %s", e.getType(), e.getReason()));
                //TODO Figure out which error message to print to console & when
                System.out.println("EchoClient> Error: " + e.getReason());
            }
            
        }
    }

    /**
     * Initiates connection of client to server using the EchoClient instance if the provided 
     * arguments are in the correct format.
     * 
     * @param tokens parsed input from the console: tokens[1] is the address of the server and tokens[2] is the port number
     * @throws ClientException if the connection is unsuccessful
     * @throws NumberFormatException if port number is not an integer
     */
    private static void connect( String[] tokens) throws ClientException{
        try {
            port = Integer.parseInt(tokens[2]);
            address = tokens[1];

            //create new connection and receive confirmation from server
            LOGGER.info(String.format("Initiating connection to %s:%d", address, port));
            byte[] response = client.connectAndReceive(address, port);
            String confirmation = new String( response, 0, response.length - 2, Constants.TELNET_ENCODING);
            System.out.println("server> " + confirmation);
            LOGGER.info(String.format("Connection to %s:%d successful.", address, port));

        } catch (NumberFormatException e) {
            LOGGER.info("Unrecognized command. Port number in wrong format.");
            printHelp();
        } catch (UnsupportedEncodingException e){
            LOGGER.severe("UnsupportedEncodingException when trying to convert server message to String type.");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Called after sending a message to the server. Receives server's response in bytes, coverts it to String
     * in proper encoding and prints it to console.
     * @throws ClientException
     */
    private static void receiveMessage() throws ClientException{
        try {
            //receive and print server response
            LOGGER.info("Receiving message from server.");
            byte[] response = client.receive();
            String responseStr = new String( response, 0, response.length - 2, Constants.TELNET_ENCODING);
            System.out.println("server> " + responseStr);
            
        } catch (UnsupportedEncodingException e){
            LOGGER.severe("UnsupportedEncodingException when trying to convert server message to String type.");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Changes the logger to the specified level if valid.
     * @param input is the user input for the desired log level
     * @throws IllegalArgumentException if the parameter is not a valid logger Level name 
     */
    private static void changeLogLevel(String input){
        try{
            String oldLevel = LOGGER.getLevel().getName();
            Level newLevel = Level.parse(input);
            LOGGER.setLevel(newLevel);

            LOGGER.info( String.format("Loglevel set from %s to %s.", oldLevel, input));
            System.out.println( String.format("Loglevel set from %s to %s.", oldLevel, input));
        } catch(IllegalArgumentException e){
            LOGGER.severe( String.format("Log level %s not valid.", input));
            printHelp();
        }
    }

    /**
     * Prints information about the intended usage of the client application and describes its set of commands.
     */
    private static void printHelp(){
        System.out.println("Possible commands:");
        System.out.printf("%-30s: %s%n", "connect <address> <port>","establishes a connection to <address>:<port>");
        System.out.printf("%-30s: %s%n", "disconnect", "to disconnect from existing connection");
        System.out.printf("%-30s: %s%n", "send <message>", "sends <message> to the server and receives a response");
        System.out.printf("%-30s: %s%n", "logLevel <new_level>", "changes the logging level to the <new_level>");
        System.out.printf("%-30s: %s%n", "quit", "closes the interface");
        System.out.printf("%-30s: %s%n", "help", "gives information aboutcommands");
    }

}