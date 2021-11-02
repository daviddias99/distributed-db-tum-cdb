package de.tum.i13.client;

import de.tum.i13.client.exceptions.ClientException;
import de.tum.i13.shared.Constants;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

public class Shell{    

    private final EchoClient client;
    private static final Logger LOGGER = LogManager.getLogger(Shell.class.getName());

    private String address;              //server address of current connection, for logging purposes
    private int port;                    //port number of current connection
    private boolean quit;

    /**
     * Creates a new command line interface.
     */
    public Shell(){
        client = new EchoClient();
        quit = false;
    }

    /**
     * Main method. Creates a shell and starts it.
     */
    public static void main(String[] args) throws IOException {
        Shell shell = new Shell();
        shell.start();
    }

    /**
     * Starts shell. Reads user commands from the console and maintains connection of our Client to a Server.
     * Client can connect to {@code <address>:<port>} , disconnect, send a message to the server, change logging level.
     */
    public void start() throws IOException   {
        BufferedReader cons = new BufferedReader(new InputStreamReader(System.in));
        quit = false;
        while (!quit) {
            //print prompt
            System.out.print(Constants.PROMPT);

            //read user input from console
            String input = cons.readLine();
            String[] tokens = input.trim().split("\\s+");

            try {
                handleInput(input, tokens);
            } catch (ClientException e) {
                handleClientException(e);
            }
        }
    }

    private void handleInput(String input, String[] tokens) throws ClientException {
        //connect command should be in format: "connect <address> <port>"
        if( tokens.length == 3 && tokens[0].equals(Constants.CONNECT_COMMAND)){
            connect(tokens);
        }
        //disconnect command should provide status report upon successful disconnection
        else if( tokens.length == 1 && tokens[0].equals(Constants.DISCONNECT_COMMAND)){
            disconnect();
        }
        //command to send message to server: "send <message>"
        else if(tokens[0].equals(Constants.SEND_COMMAND)){
            send(input);
        }
        //command to change the logging level: "logLevel <level>"
        else if(tokens.length == 2 && tokens[0].equals(Constants.LOG_COMMAND)){
            changeLogLevel(tokens[1]);
        }
        //help command to print information about the program
        else if(tokens.length == 1 && tokens[0].equals(Constants.HELP_COMMAND)){
            printHelp();
        }
        //quit command should close any existing connection before quitting the program
        else if(tokens.length == 1 && tokens[0].equals(Constants.QUIT_COMMAND)){
            quit();
        }
        //unrecognized input
        else{
            handleFaultyCommand("Unrecognized command.");
        }
    }

    /**
     * Disconnects from the server
     * @throws ClientException in case the disconnect is unsuccessful
     */
    private void disconnect() throws ClientException {
        LOGGER.info("Disconnecting from {}:{}", address, port);
        client.disconnect();
        System.out.println(Constants.PROMPT + "Successfully disconnected.");
    }

    /**
     * Send the input to the server
     * @param input the input to send
     * @throws ClientException in case the sending process is unsuccessful
     */
    private void send(String input) throws ClientException {
        //send the message in bytes after appending the delimiter
        LOGGER.info("Sending message to {}:{}", address, port);
        client.send( (input.substring(5) + Constants.TERMINATING_STR).getBytes());
        receiveMessage();
    }

    /**
     * Quits the shell
     * @throws ClientException in case the quitting process is unsuccessful
     */
    private void quit() throws ClientException {
        if(client.isConnected()){
            LOGGER.info("Disconnecting from {}:{}", address, port);
            client.disconnect();
        }

        LOGGER.info("Quitting application.");
        System.out.println(Constants.PROMPT + "Application exit!");
        quit = true;
    }

    /**
     * Handles a faulty command
     * @param reason the reasons why the command is faulty
     */
    private void handleFaultyCommand(String reason) {
        LOGGER.info(reason);
        printHelp();
    }

    /**
     * Handles a {@link ClientException}
     * @param e the exception to handle
     */
    private void handleClientException(ClientException e) {
        LOGGER.error("Exception type: {}. Exception reason: {}", e.getType(), e.getReason());
        //TODO Figure out which error message to print to console & when
        System.out.println(Constants.PROMPT + "Error: " + e.getReason());
    }

    /**
     * Initiates connection of client to server using the EchoClient instance if the provided 
     * arguments are in the correct format.
     * 
     * @param tokens parsed input from the console: tokens[1] is the address of the server and tokens[2] is the port number
     * @throws ClientException if the connection is unsuccessful
     * @throws NumberFormatException if port number is not an integer
     */
    private void connect( String[] tokens) throws ClientException{
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
            handleFaultyCommand("Unrecognized command. Port number in wrong format.");
        }
    }

    /**
     * Called after sending a message to the server. Receives server's response in bytes, coverts it to String
     * in proper encoding and prints it to console.
     * @throws ClientException
     */
    private void receiveMessage() throws ClientException{
        //receive and print server response
        LOGGER.info("Receiving message from server.");
        byte[] response = client.receive();
        String responseStr = new String( response, 0, response.length - 2, Constants.TELNET_ENCODING);
        System.out.println(Constants.PROMPT + responseStr);

    }

    /**
     * Changes the logger to the specified level if valid.
     * @param input is the user input for the desired log level
     * @throws IllegalArgumentException if the parameter is not a valid logger Level name 
     */
    private void changeLogLevel(String input){
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
    private void printHelp(){
        System.out.println("Possible commands:");
        System.out.printf("%-30s: %s%n", Constants.CONNECT_COMMAND + " <address> <port>","establishes a connection to <address>:<port>");
        System.out.printf("%-30s: %s%n", Constants.DISCONNECT_COMMAND , "to disconnect from existing connection");
        System.out.printf("%-30s: %s%n", Constants.SEND_COMMAND + " <message>", "sends <message> to the server and receives a response");
        System.out.printf("%-30s: %s%n", Constants.LOG_COMMAND + " <new_level>", "changes the logging level to the <new_level>. Possible values: " + Arrays.toString(Level.values()));
        System.out.printf("%-30s: %s%n", Constants.QUIT_COMMAND , "closes the interface");
        System.out.printf("%-30s: %s%n", Constants.HELP_COMMAND , "gives information aboutcommands");
    }

}