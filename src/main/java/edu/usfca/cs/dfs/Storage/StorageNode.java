package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Coordinator.HashRing;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.DataSender.DataRequesterWithAck;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.math.BigInteger;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;



public class StorageNode extends Thread{

    private ConcurrentHashMap<String,Chunk> chunk_storage = new ConcurrentHashMap<>();
    private HashRing<byte[]> hashRing;
    private SHA1 sha1 = new SHA1();
    private String ipaddress;
    private int port;
    private boolean run;
    private Heartbeat heartbeat;

    public StorageNode(int port,String coordip,int coordport){
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
        this.run = request_access(coordip,coordport);
        heartbeat = new Heartbeat(hashRing.get_size(),ipaddress+Integer.toString(port),coordip,coordport);
        heartbeat.start();
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname()
    throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }

    @Override
    public String toString() {
        return ipaddress + Integer.toString(port);
    }

    @Override
    public void start(){
        startNode();
    }

    public void startNode()
    {
        System.out.println("Server Started");
        while (run){
            try(
                ServerSocket serverSocket = new ServerSocket(this.port);
            ){
                store_chunk_listener scl = new store_chunk_listener(serverSocket.accept());
                scl.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Server Ending");
    }

    private class store_chunk_listener extends Thread{
        private Socket s;
        public store_chunk_listener(Socket s){
            this.s = s;
        }

        @Override
        public synchronized void run() {
            System.out.println(s.getInetAddress() + "  " + Integer.toString(s.getPort()));
            try {
                InputStream instream = s.getInputStream();
                OutputStream outputStream = s.getOutputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);

                if(dataPacket.hasRequest()) {
                    System.out.println("Request Recieved");
                    process_request(dataPacket.getRequest(), s);
                }
                else if(dataPacket.hasHashringentry()) {
                    process_hre(dataPacket);
                }else if(dataPacket.hasSinglechunk()){
                    process_single_chunk(dataPacket, s);
                }else if(dataPacket.hasChunklife()){
                    pipline_update(dataPacket);
                }
                StorageMessages.Ack ack = StorageMessages.Ack.newBuilder().setAck(true).build();
                dataPacket = StorageMessages.DataPacket.newBuilder().setAck(ack).build();
                dataPacket.writeDelimitedTo(outputStream);

                s.close();
            }catch(IOException e) {

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    /*
    Function used to proccess hash ring request
     */
    private void process_hre(StorageMessages.DataPacket dataPacket)
    {

        StorageMessages.HashRingEntry hashRingEntry = dataPacket.getHashringentry();
        try {
            if(hashRingEntry.getAdd()) {
                System.out.println("adding node");
                hashRing.addNodePos(new BigInteger(hashRingEntry.getPosition().toByteArray()), hashRingEntry.getIpaddress(), hashRingEntry.getPort());
            }
            else if(!hashRingEntry.getAdd()) {
                System.out.println("removing node");
                hashRing.remove_node(new BigInteger(hashRingEntry.getPosition().toByteArray()));
            }
        } catch (HashTopologyException e) {
            e.printStackTrace();
        }
    }

    /*
    Request access from the coordinator.
     */
    private boolean request_access(String ipaddress, int port)
    {
        System.out.println("requesting access");
        try (
                Socket s = new Socket(ipaddress,port);
                OutputStream outputStream = s.getOutputStream();
                InputStream inputStream = s.getInputStream();
        ){
            CoordMessages.RequestEntry requestEntry = CoordMessages.RequestEntry.newBuilder()
                    .setIpaddress(this.ipaddress)
                    .setPort(this.port)
                    .build();
            CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                    .setRequestentry(requestEntry)
                    .build();
            dataPacket.writeDelimitedTo(outputStream);
            CoordMessages.DataPacket response = CoordMessages.DataPacket.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);
            hashRing = new HashRing(sha1,response.getHashring());
            System.out.println(hashRing.toString());
            return true;

        }catch (IOException ioe){
            ioe.printStackTrace();
        }

        return false;
    }

    /*
    Process request packets
    -   Storage packets
    -   Get requests
     */
    private void process_request(StorageMessages.Request r_chunk,Socket s) throws InterruptedException {
        Chunk s_chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
        System.out.println(chunk_storage.toString());
        if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.store_chunk) {
            try {
                store_chunk(r_chunk);
            } catch (HashException e) {
                e.printStackTrace();
            }
            System.out.println(s_chunk.getChunk_id());
            System.out.println(s_chunk.getFile_name());
            System.out.println(s_chunk.getData_chunk().length);
        }
        else if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_chunk) {
            String key = key_gen(r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
            get_chunk(key, r_chunk.getIpaddress(),r_chunk.getPort());
        }


    }
    /*
    processes a single chunk to send back to cliet
     */
    public void get_chunk(String key,String ipaddress,int port){
        if(chunk_storage.containsKey(key))
        {
            Chunk chunk = chunk_storage.get(key);
            ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(), 0, chunk.getData_chunk().length);

            StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                    .setChunkNumber(chunk.getChunk_id())
                    .setFileName(chunk.getFile_name())
                    .setIsLast(chunk.getIs_last())
                    .setData(bsval)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setSinglechunk(singleChunk)
                    .build();
            DataRequester dataRequester = new DataRequester(dataPacket,ipaddress,port);
            dataRequester.start();
        }
    }


    public void store_chunk(StorageMessages.Request r_chunk) throws HashException {
        Chunk chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
        String key = key_gen(chunk.getFile_name(),chunk.getChunk_id(),chunk.getIs_last());
        BigInteger pos = hashRing.locate(key.getBytes());
        HashRingEntry node = hashRing.returnNode(pos);
        if(node.inetaddress.equals(this.ipaddress) && node.port == this.port) {
            chunk_storage.put(key, chunk);

            StorageMessages.ChunkLife chunkLife = StorageMessages.ChunkLife.newBuilder()
                    .setSingleChunk(request_to_chunk(r_chunk))
                    .setLife(2)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setChunklife(chunkLife)
                    .build();
            HashRingEntry hre = get_next_neighbor(key,2);
            if(hre.inetaddress != this.ipaddress && hre.port != this.port) {
                DataRequesterWithAck dataRequester = new DataRequesterWithAck(dataPacket, hre.inetaddress, hre.port);
                dataRequester.start();
            }
        }
        else {
           forward_chunk(chunk,node,r_chunk);
        }
    }

    private HashRingEntry get_next_neighbor(String key,int life) throws HashException {
        BigInteger location = hashRing.locate(key.getBytes());
        HashRingEntry hre = null;
        for(int i = 1; i <= life; i++){
            hre = hashRing.get_next_entry(location);
            location = hre.position;
        }
        return hre;
    }

    private StorageMessages.SingleChunk request_to_chunk(StorageMessages.Request request)
    {
        return StorageMessages.SingleChunk.newBuilder()
                .setData(request.getData())
                .setWrite(true)
                .setIsLast(request.getIslast())
                .setFileName(request.getFileName())
                .setChunkNumber(request.getChunkId())
                .build();
    }

    private void forward_chunk(Chunk chunk, HashRingEntry node,StorageMessages.Request r_chunk) throws HashException{
        System.out.println("Storing on external node " + chunk.getFile_name() + Integer.toString(chunk.getChunk_id()));
        System.out.println(node.inetaddress + " " + Integer.toString(node.port));

        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(r_chunk)
                .build();
        DataRequester dataRequester = new DataRequester(dataPacket,node.inetaddress,node.port);
        dataRequester.start();
    }

    private void pipline_update(StorageMessages.DataPacket dataPacket){
        System.out.println("pipline updating chunk life : " + Integer.toString(dataPacket.getChunklife().getLife()));
        StorageMessages.ChunkLife chunkLife = dataPacket.getChunklife();
        chunkLife = StorageMessages.ChunkLife.newBuilder()
                .setLife(chunkLife.getLife() - 1)
                .setSingleChunk(chunkLife.getSingleChunk())
                .build();
        StorageMessages.SingleChunk singleChunk = chunkLife.getSingleChunk();
        Chunk chunk = new Chunk(singleChunk.getData().toByteArray(),singleChunk.getFileName(),singleChunk.getChunkNumber(),singleChunk.getIsLast());
        String key = key_gen(chunk.getFile_name(),chunk.getChunk_id(),chunk.getIs_last());
        if(!chunk_storage.containsKey(key)) {
            chunk_storage.put(key, chunk);
            if (chunkLife.getLife() > 0) {
                StorageMessages.DataPacket sendpacket = StorageMessages.DataPacket.newBuilder().setChunklife(chunkLife).build();
                try {
                    HashRingEntry hre = get_next_neighbor(key, chunkLife.getLife());
                    DataRequesterWithAck dataRequester = new DataRequesterWithAck(sendpacket, hre.inetaddress, hre.port);
                    dataRequester.start();
                } catch (HashException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    private String key_gen(String filename, int chunkid, boolean islast){
        if(islast)
            return filename + "last";
        else
            return filename + Integer.toString(chunkid);
    }

    private void process_single_chunk(StorageMessages.DataPacket dataPacket, Socket s){
        StorageMessages.SingleChunk singleChunk = dataPacket.getSinglechunk();
        try (
                OutputStream outputStream = s.getOutputStream();
        ) {
            Chunk chunk = chunk_storage.get(singleChunk.getFileName()+"last");
            ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(), 0, chunk.getData_chunk().length);
            System.out.println("sending file: " + chunk.getFile_name());
            singleChunk = StorageMessages.SingleChunk.newBuilder()
                    .setChunkNumber(chunk.getChunk_id())
                    .setFileName(chunk.getFile_name())
                    .setIsLast(chunk.getIs_last())
                    .setData(bsval)
                    .build();
            dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setSinglechunk(singleChunk)
                    .build();
            dataPacket.writeDelimitedTo(outputStream);
        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }




    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode(5000,"localhost",6000);
        storageNode.startNode();

    }



}
