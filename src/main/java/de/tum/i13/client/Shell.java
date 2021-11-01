package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Optional;

public class Shell{    

    private static EchoClient client = new EchoClient();
    private static final Logger LOGGER = LogManager.getLogger(Shell.class.getName());

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

        BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
        boolean quit = false;

        while (!quit) {
            //print prompt 
            System.out.print(Constants.PROMPT);

            //read user input from console
            String input = cons.readLine();
            String[] tokens = input.trim().split("\\s+");

            try {
                //connect command should be in format: "connect <address> <port>"
                if( tokens.length == 3 && tokens[0].equals(Constants.CONNECT_COMMAND)){
                    connect(tokens);
                }
                //disconnect command should provide status report upon successful disconnection
                else if( tokens.length == 1 && tokens[0].equals(Constants.DISCONNECT_COMMAND)){
                    LOGGER.info("Disconnecting from {}:{}", address, port);
                    client.disconnect();
                    System.out.println(Constants.PROMPT + "Successfully disconnected.");
                }
                //command to send message to server: "send <message>"
                else if(tokens[0].equals(Constants.SEND_COMMAND)){
                    //send the message in bytes after appending the delimiter
                    LOGGER.info("Sending message to {}:{}", address, port);
                    client.send( (input.substring(5) + Constants.TERMINATING_STR).getBytes());
                    receiveMessage();
                }
                //command to change the logging level: "logLevel <level>"
                else if( tokens.length == 2 && tokens[0].equals(Constants.LOG_COMMAND)){
                    changeLogLevel(tokens[1]);
                }
                //help command to print information about the program
                else if( tokens.length == 1 && tokens[0].equals(Constants.HELP_COMMAND)){
                    printHelp();
                }
                //quit command should close any existing connection before quitting the program
                else if( tokens.length == 1 && tokens[0].equals(Constants.QUIT_COMMAND)){
                    if(client.isConnected()){
                        LOGGER.info("Disconnecting from {}:{}", address, port);
                        client.disconnect();
                    }

                    LOGGER.info("Quitting application.");
                    quit = true;
                    System.out.println(Constants.PROMPT + "Application exit!");
                }
                //unrecognized input
                else{
                    LOGGER.info("Unrecognized command.");
                    printHelp();
                }
            } catch (ClientException e) {
                LOGGER.error("Exception type: {}. Exception reason: {}", e.getType(), e.getReason());
                //TODO Figure out which error message to print to console & when
                System.out.println(Constants.PROMPT + "Error: " + e.getReason());
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
            LOGGER.info("Initiating connection to {}:{}", address, port);
            byte[] response = client.connectAndReceive(address, port);
            String confirmation = new String( response, 0, response.length - 2, Constants.TELNET_ENCODING);
            System.out.println(Constants.PROMPT + confirmation);
            LOGGER.info("Connection to {}:{} successful.", address, port);

        } catch (NumberFormatException e) {
            LOGGER.info("Unrecognized command. Port number in wrong format.");
            printHelp();
        } catch (UnsupportedEncodingException e){
            LOGGER.error("UnsupportedEncodingException when trying to convert server message to String type.");
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
            System.out.println(Constants.PROMPT + responseStr);
            
        } catch (UnsupportedEncodingException e){
            LOGGER.error("UnsupportedEncodingException when trying to convert server message to String type.");
            System.out.println(e.getMessage());
        }
    }

    /**
     * Changes the logger to the specified level if valid.
     * @param input is the user input for the desired log level
     * @throws IllegalArgumentException if the parameter is not a valid logger Level name 
     */
    private static void changeLogLevel(String input){
        Optional<Level> newLevel = Optional.ofNullable(Level.getLevel(input));
        if (newLevel.isPresent()) {
            String oldLevelName = LOGGER.getLevel().name();
            Configurator.setLevel(LogManager.getLogger(Shell.class).getName(), newLevel.get());
            LOGGER.info( "Log level set from {} to {}.", oldLevelName, input);
            System.out.printf("Log level set from %s to %s.%n", oldLevelName, input);
        }
        else {
            LOGGER.error( "Log level {} not valid.", input);
            printHelp();
        }
    }

    /**
     * Prints information about the intended usage of the client application and describes its set of commands.
     */
    private static void printHelp(){
        System.out.println("Possible commands:");
        System.out.printf("%-30s: %s%n", Constants.CONNECT_COMMAND + " <address> <port>","establishes a connection to <address>:<port>");
        System.out.printf("%-30s: %s%n", Constants.DISCONNECT_COMMAND , "to disconnect from existing connection");
        System.out.printf("%-30s: %s%n", Constants.SEND_COMMAND + " <message>", "sends <message> to the server and receives a response");
        System.out.printf("%-30s: %s%n", Constants.LOG_COMMAND + " <new_level>", "changes the logging level to the <new_level>");
        System.out.printf("%-30s: %s%n", Constants.QUIT_COMMAND , "closes the interface");
        System.out.printf("%-30s: %s%n", Constants.HELP_COMMAND , "gives information aboutcommands");
    }

}