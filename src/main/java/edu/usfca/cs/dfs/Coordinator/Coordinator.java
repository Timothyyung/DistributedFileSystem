package edu.usfca.cs.dfs.Coordinator;


import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Coordinator extends Thread{

    private HashRing<byte[]> hashRing;


    public Coordinator()
    {
        SHA1 sha1 = new SHA1();
        hashRing = new HashRing<>(sha1);
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
                    Socket sock = serverSocket.accept()
            ){

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class coordinator_listener extends Thread{
        private Socket s;
        public coordinator_listener(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
            try {
                InputStream instream = s.getInputStream();
                CoordMessages.RequestEntry entryRequest = CoordMessages.RequestEntry.parseDelimitedFrom(instream);
                try {
                    hashRing.addNode(entryRequest.getIpaddress(),entryRequest.getPort());
                } catch (HashTopologyException e) {
                    e.printStackTrace();
                } catch (HashException e) {
                    e.printStackTrace();
                }


                s.close();
            }catch(IOException e)
            {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {

        System.out.println("Starting coordinator...");



    }

}
