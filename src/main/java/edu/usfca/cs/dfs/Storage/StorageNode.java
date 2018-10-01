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
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;



public class StorageNode extends Thread{

    private ConcurrentHashMap<String,Chunk> chunk_storage = new ConcurrentHashMap<>();
    private HashRing<byte[]> hashRing;
    private SHA1 sha1 = new SHA1();
    private String ipaddress;
    private int port;
    private boolean finished;


    public StorageNode(int port){
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
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
    public void run(){
        startNode();
    }

    public void startNode()
    {
        boolean run = request_access("localhost",6000);

        System.out.println("Server Started");
        while (run){
            try(
                ServerSocket serverSocket = new ServerSocket(this.port);
                Socket sock = serverSocket.accept();
            ){
                store_chunk_listener scl = new store_chunk_listener(sock);
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
        public synchronized void start() {
            System.out.println(s.getInetAddress() + "  " + Integer.toString(s.getPort()));
            try {
                InputStream instream = s.getInputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);

                if(dataPacket.hasRequest())
                    process_request(dataPacket.getRequest(),s);
                else if(dataPacket.hasHashringentry()) {
                    process_hre(dataPacket);
                }else if(dataPacket.hasSinglechunk()){
                    process_single_chunk(dataPacket, s);
                }
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
        System.out.println("adding node");
        StorageMessages.HashRingEntry hashRingEntry = dataPacket.getHashringentry();
        try {
            hashRing.addNodePos(new BigInteger(hashRingEntry.getPosition().toByteArray()),hashRingEntry.getIpaddress(),hashRingEntry.getPort());
        } catch (HashTopologyException e) {
            e.printStackTrace();
        }
    }

    /*
    Request access from the coordinator.
     */
    private boolean request_access(String ipaddress, int port)
    {
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

        else if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_data){
            System.out.println("Request Accepted");
            data_reply(r_chunk.getFileName(), r_chunk.getIpaddress(),r_chunk.getPort());
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
        System.out.println(key);
        System.out.println(hashRing.toString());
        BigInteger pos = hashRing.locate(key.getBytes());
        HashRingEntry node = hashRing.returnNode(pos);
        System.out.println(pos);
        if(node.inetaddress.equals(this.ipaddress) && node.port == this.port)
            chunk_storage.put(key,chunk);
        else {
            System.out.println("Storing on external node " + chunk.getFile_name() + Integer.toString(chunk.getChunk_id()));
            System.out.println(node.inetaddress + " " + Integer.toString(node.port));

            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setRequest(r_chunk)
                    .build();
            DataRequester dataRequester = new DataRequester(dataPacket,node.inetaddress,node.port);
            dataRequester.start();
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

    private void data_reply(String filename, String ipaddress, int port) throws InterruptedException {
        int i = 1;
       finished = false;
        while(!finished)
        {
            Thread.sleep(1000);

            try {
                search_and_send(filename,i,ipaddress,port);
            } catch (HashException e) {
                e.printStackTrace();
            }
            i += 1;
        }
    }

    private void search_and_send(String filename,int chunknumber, String ipaddress,int port) throws HashException {
        boolean last = false;
        String key = filename + Integer.toString(chunknumber);
        if(chunk_storage.containsKey(key)){
            System.out.println("I have this chunk");
            last = send_to_node(key,ipaddress,port);
        }else {
            System.out.println("I dont having this chunk");
            request_from_storage(filename,chunknumber,ipaddress, port);
        }
        System.out.println("data chunk " + filename + Integer.toString(chunknumber)+ "sent");
        finished = last;
    }

    private boolean send_to_node(String filekey,String ipaddress, int port){
            Chunk chunk = chunk_storage.get(filekey);
            ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(), 0, chunk.getData_chunk().length);
            System.out.println("sending file: " + chunk.getFile_name());
            StorageMessages.SingleChunk singleChunk = StorageMessages.SingleChunk.newBuilder()
                    .setChunkNumber(chunk.getChunk_id())
                    .setFileName(chunk.getFile_name())
                    .setIsLast(chunk.getIs_last())
                    .setData(bsval)
                    .build();
            StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                    .setSinglechunk(singleChunk)
                    .build();
            DataRequester sender = new DataRequester(dataPacket,ipaddress,port);
            sender.start();
            return chunk.getIs_last();
        }

    private void request_from_storage(String filename, int chunknumber,String ipaddress, int port) throws HashException {
        StorageMessages.Request request = StorageMessages.Request.newBuilder()
                .setOpcode(StorageMessages.Request.Op_code.get_chunk)
                .setFileName(filename)
                .setChunkId(chunknumber)
                .setIpaddress(ipaddress)
                .setPort(port)
                .build();
        StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                .setRequest(request)
                .build();
        BigInteger pos = hashRing.locate((filename+Integer.toString(chunknumber)).getBytes());
        HashRingEntry node = hashRing.returnNode(pos);
        DataRequester dataRequester = new DataRequester(dataPacket,node.inetaddress ,node.port);
        dataRequester.start();

    }



    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode(5070);
        storageNode.startNode();

    }



}
