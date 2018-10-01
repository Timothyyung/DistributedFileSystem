package edu.usfca.cs.dfs.Coordinator;


import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Coordinator extends Thread{

    private HashRing<byte[]> hashRing;
    private HashMap<String,BigInteger> node_map;

    public Coordinator() throws HashTopologyException, HashException {
        SHA1 sha1 = new SHA1();
        hashRing = new HashRing<>(sha1);
        node_map = new HashMap<>();
        //make_hash();
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
                coordinator_listener cl = new coordinator_listener(sock);
                cl.run();
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
                OutputStream outputStream = s.getOutputStream();
                CoordMessages.DataPacket request = CoordMessages.DataPacket.parseDelimitedFrom(instream);
                System.out.println("Data Packet Accepted");
                if(request.hasRequestentry())
                {
                    process_entry(request,outputStream);
                }
                else if(request.hasRequestmap()){
                    System.out.println("hash map request");
                    process_map_request(outputStream);
                }
                s.close();
            }catch(IOException | HashTopologyException | HashException e)
            {
                e.printStackTrace();
            }
        }

        private void process_map_request(OutputStream outputStream) throws IOException {
            CoordMessages.DataPacket response = CoordMessages.DataPacket.newBuilder()
                    .setHashring(hashRing.treemap_to_map())
                    .build();
            response.writeDelimitedTo(outputStream);
        }

        private void process_entry(CoordMessages.DataPacket dataPacket, OutputStream outputStream) throws HashTopologyException, HashException, IOException {
            CoordMessages.RequestEntry entryRequest = dataPacket.getRequestentry();
            CoordMessages.HashRingEntry hashRingEntry = put_in_map(entryRequest);

            hashRing.sendUpdate(entryRequest.getIpaddress(),entryRequest.getPort(),hashRingEntry);

            CoordMessages.DataPacket response = CoordMessages.DataPacket.newBuilder()
                    .setHashring(hashRing.treemap_to_map())
                    .build();
            response.writeDelimitedTo(outputStream);
        }

        private CoordMessages.HashRingEntry put_in_map(CoordMessages.RequestEntry entryRequest) throws HashException, HashTopologyException {

            BigInteger pos;
            if(!node_map.containsKey(entryRequest.getIpaddress()+Integer.toString(entryRequest.getPort()))) {
                pos = hashRing.addNode(entryRequest.getIpaddress(),entryRequest.getPort());
                System.out.println("adding new node " + pos);
                node_map.put(entryRequest.getIpaddress() + Integer.toString(entryRequest.getPort()), pos);
                System.out.println(node_map.toString());
            }else{
                pos = node_map.get(entryRequest.getIpaddress()+Integer.toString(entryRequest.getPort()));
            }

            ByteString bsval = ByteString.copyFrom(pos.toByteArray(), 0, pos.toByteArray().length);
            CoordMessages.HashRingEntry hashRingEntry = CoordMessages.HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setIpaddress(entryRequest.getIpaddress())
                    .setPort(entryRequest.getPort())
                    .build();

            return hashRingEntry;
        }


    }


     private void make_hash() throws  HashException, HashTopologyException
     { node_map.put("localhost2020",hashRing.addNode("localhost", 2020));
         node_map.put("localhost2030",hashRing.addNode("localhost", 2030));
         node_map.put("localhost2040",hashRing.addNode("localhost", 2040));
         node_map.put("localhost2050",hashRing.addNode("localhost", 2050));

     }


    public static void main(String[] args) {

        System.out.println("Starting coordinator on localhost");
        try {
            Coordinator coordinator = new Coordinator();
            coordinator.startCoord();
        } catch (HashTopologyException e) {
            e.printStackTrace();
        } catch (HashException e) {
            e.printStackTrace();
        }


    }

}
