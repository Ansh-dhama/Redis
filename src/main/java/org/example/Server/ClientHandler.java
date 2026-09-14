package org.example.Server;

import org.example.Protocol.RespParser;
import org.example.commond.CommandDispatcher;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandDispatcher  dispatcher;

    public ClientHandler(Socket socket,  CommandDispatcher dispatcher) {
        this.socket = socket;
        this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
        RespParser respParser = new RespParser();
        try (socket;
             InputStream reader = socket.getInputStream();
             OutputStream output = socket.getOutputStream()) {
            while (true) {

                List<String> result = respParser.parse(reader);

                if (result == null) break;
                byte[] response = dispatcher.dispatch(result);
                output.write(response);
                output.flush();

            }
        } catch (IOException e) {
            System.out.println(
                    "Client disconnected: "
                            + e.getMessage()
            );
        }

    }
}
