package de.tum.i13.server.threadperconnection;

import de.tum.i13.server.Config;
import de.tum.i13.server.kv.KVCommandProcessor;
import de.tum.i13.server.kv.KVStoreStub;
import de.tum.i13.shared.CommandProcessor;
import de.tum.i13.shared.Constants;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static de.tum.i13.shared.LogSetup.setupLogging;

/**
 * Created by chris on 09.01.15.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        Config cfg = Config.parseCommandlineArgs(args);  //Do not change this
        setupLogging(cfg.logfile, cfg.logLevel);

        final ServerSocket serverSocket = new ServerSocket();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Closing thread per connection kv server");
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        //bind to localhost only
        serverSocket.bind(new InetSocketAddress(cfg.listenAddress, cfg.port));

        //Replace with your Key value server logic.
        // If you use multithreading you need locking
        CommandProcessor logic = new KVCommandProcessor(new KVStoreStub());

        //Use ThreadPool
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.CORE_POOL_SIZE);

        try{
            while (true) {
                //accept a connection
                Socket clientSocket = serverSocket.accept();
    
                //start a new Thread for this connection
                executorService.submit(new ConnectionHandleThread(logic, clientSocket, (InetSocketAddress) serverSocket.getLocalSocketAddress()));
            }
        } catch(IOException e){
            executorService.shutdown();
        }
    }
}
