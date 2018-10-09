package edu.usfca.cs.dfs.Coordinator;


import com.google.protobuf.ByteString;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.DataSender.DataRequesterWithAck;
import edu.usfca.cs.dfs.StorageMessages;

import javax.xml.soap.Node;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class Coordinator{

    private HashRing<byte[]> hashRing;
    private HashMap<String,NodeTimer> node_map;
    private String ipaddress;
    private int port;
    private  SHA1 sha1 = new SHA1();
    public Coordinator() {

        hashRing = new HashRing<>(sha1);
        node_map = new HashMap<>();
        this.ipaddress = "localhost";
        this.port = 6000;
    }

    public void startCoord()
    {
        boolean run = true;
        System.out.println("Server Started");
        while (run){
            try(
                    ServerSocket serverSocket = new ServerSocket(port);
                    Socket sock = serverSocket.accept()
            ){
                coordinator_listener cl = new coordinator_listener(sock, this.ipaddress,this.port);
                cl.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class coordinator_listener extends Thread{
        private Socket s;
        private String ip;
        private int port;
        public coordinator_listener(Socket s,String ip, int port){
            this.s = s;
            this.ip= ip;
            this.port = port;
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
                }else if(request.hasRemovenode())
                {
                    System.out.println("removing node: " + request.getRemovenode().getKey());
                    remove_node(request.getRemovenode().getKey());
                    System.out.println(hashRing.toString());
                }else if(request.hasHeartbeat()) {
                    System.out.println("heartbeat recieved: " + request.getHeartbeat().getNodeKey());
                    if(node_map.containsKey(request.getHeartbeat().getNodeKey()))
                        node_map.get(request.getHeartbeat().getNodeKey()).resetTime();
                    else{

                    }
                }
                s.close();
            }catch(IOException | HashTopologyException | HashException e)
            {
                e.printStackTrace();
            }
        }

        private void get_node_map() throws IOException {
            Socket s = new Socket();
            OutputStream outputStream = s.getOutputStream();
            InputStream inputStream = s.getInputStream();

            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setHashring(StorageMessages.HashRing.getDefaultInstance())
                    .build();
            dataPacket.writeDelimitedTo(outputStream);

            CoordMessages.DataPacket response = CoordMessages.DataPacket.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);

            hashRing.map_to_treemap(response.getHashring());

        }


        private void process_map_request(OutputStream outputStream) throws IOException {
            CoordMessages.DataPacket response = CoordMessages.DataPacket.newBuilder()
                    .setHashring(hashRing.treemap_to_map())
                    .build();
            response.writeDelimitedTo(outputStream);
        }

        private void remove_node(String key){
            BigInteger rpos = node_map.get(key).getPos();
            ByteString bsval = ByteString.copyFrom(rpos.toByteArray(), 0, rpos.toByteArray().length);

            node_map.remove(key);
            hashRing.remove_node(rpos);
            StorageMessages.HashRingEntry hashRingEntry = StorageMessages.HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setAdd(false)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setHashringentry(hashRingEntry)
                    .build();


            for (Map.Entry<BigInteger, HashRingEntry> entry: hashRing.getMap().entrySet()){
                DataRequesterWithAck dataRequesterWithAck = new DataRequesterWithAck(dataPacket,entry.getValue().inetaddress,entry.getValue().port);
                dataRequesterWithAck.start();
            }
        }

        private void process_entry(CoordMessages.DataPacket dataPacket, OutputStream outputStream) throws HashTopologyException, HashException, IOException {
            CoordMessages.RequestEntry entryRequest = dataPacket.getRequestentry();
            CoordMessages.HashRingEntry hashRingEntry = put_in_map(entryRequest);

            CoordMessages.DataPacket response = CoordMessages.DataPacket.newBuilder()
                    .setHashring(hashRing.treemap_to_map(hashRingEntry))
                    .build();
            response.writeDelimitedTo(outputStream);
            hashRing.sendUpdate(entryRequest.getIpaddress(),entryRequest.getPort(),hashRingEntry);
            System.out.println("update sent");
        }

        private CoordMessages.HashRingEntry put_in_map(CoordMessages.RequestEntry entryRequest) throws HashException, HashTopologyException {

            BigInteger pos;
            String key = entryRequest.getIpaddress()+Integer.toString(entryRequest.getPort());
            if(!node_map.containsKey(key)){
                pos = hashRing.addNode(entryRequest.getIpaddress(),entryRequest.getPort());
                System.out.println("adding new node " + pos);
                NodeTimer insertnode = new NodeTimer(pos,key,this.ip,this.port);
                node_map.put(key, insertnode);
                insertnode.start();

                System.out.println(node_map.toString());
            }else{
                pos = node_map.get(key).getPos();
                node_map.get(key).start();
            }

            ByteString bsval = ByteString.copyFrom(pos.toByteArray(), 0, pos.toByteArray().length);
            CoordMessages.HashRingEntry hashRingEntry = CoordMessages.HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setIpaddress(entryRequest.getIpaddress())
                    .setPort(entryRequest.getPort())
                    .setAdd(true)
                    .build();

            return hashRingEntry;
        }


    }


    public static void main(String[] args) {

        System.out.println("Starting coordinator on localhost");

        Coordinator coordinator = new Coordinator();
        coordinator.startCoord();



    }

}
