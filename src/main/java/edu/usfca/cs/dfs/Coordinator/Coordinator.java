package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.Storage.StorageNode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Coordinator extends Thread{

    public Coordinator()
    {

    }

    @Override
    public void run() {

    }

    public void startCoord()
    {
        boolean run = true;
        System.out.println("Server Started");
        while (run){
            try(
                    ServerSocket serverSocket = new ServerSocket(6000);
                    Socket sock = serverSocket.accept();
            ){

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {

        System.out.println("Starting coordinator...");



    }

}
