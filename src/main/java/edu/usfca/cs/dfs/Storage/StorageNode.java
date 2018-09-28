package edu.usfca.cs.dfs.Storage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Coordinator.HashRing;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


public class StorageNode extends Thread{

    private ConcurrentHashMap<String,Chunk> chunk_storage = new ConcurrentHashMap<>();
    private HashRing hashRing;
    private SHA1 sha1 = new SHA1();
    private ReentrantLock lock = new ReentrantLock();
    private String ipaddress;
    private int port;


    public StorageNode(){
        try {
            ipaddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

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


    public void store_chunk(Chunk chunk)
    {
       chunk_storage.put(chunk.get_hash_key(),chunk);
    }

    private int get_total_chunks(String filename)
    {
        int i = 1;

        while(chunk_storage.containsKey(filename + Integer.toString(i)))
            i += 1;
        return i - 1;
    }

    private void reassemble(String filename)
    {
        List<Byte> file = new ArrayList<>();
        for(int i = 1; i <= get_total_chunks(filename); i++)
        {
            byte[] chunk = chunk_storage.get(filename + Integer.toString(i)).getData_chunk();
            for(int j = 0; j < chunk.length ; j++)
                file.add(chunk[j]);
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("outputs");
            fos.write(toByteArray(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch ( IOException ie){
            ie.getStackTrace();
        }

    }

    private static byte[] toByteArray(List<Byte> in){
        final int n = in.size();
        byte ret[] = new byte[n];
        for (int i = 0; i < n ; i++){
            ret[i] = in.get(i);
        }
        return ret;
    }


    public Chunk get_chunk(String key){
        return chunk_storage.get(key);
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
                    process_request(dataPacket.getRequest());

                s.close();
            }catch(IOException e) {

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private boolean request_access(String ipaddress, int port)
    {
        try (
                Socket s = new Socket(ipaddress,port);
                OutputStream outputStream = s.getOutputStream();
                InputStream inputStream = s.getInputStream();
        ){
            CoordMessages.RequestEntry requestEntry = CoordMessages.RequestEntry.newBuilder()
                    .setIpaddress(ipaddress)
                    .setPort(this.port)
                    .build();
            requestEntry.writeDelimitedTo(outputStream);
            CoordMessages.Response response = CoordMessages.Response.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);

            hashRing = new HashRing(sha1,response.getHashring());
            System.out.println(hashRing.toString());
            return true;



        }catch (IOException ioe){
            ioe.printStackTrace();
        }

        return false;
    }




    private void process_request(StorageMessages.Request r_chunk) throws InterruptedException {
        Chunk s_chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId(),r_chunk.getIslast());
        if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.store_chunk) {
            store_chunk(s_chunk);
            System.out.println(s_chunk.getChunk_id());
            System.out.println(s_chunk.getFile_name());
            System.out.println(s_chunk.getData_chunk().length);
        }
        else if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_chunk) {
            System.out.println("total chunks" + get_total_chunks(r_chunk.getFileName()));
        }

        else if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_data){
            System.out.println("Request Accepted");
            data_reply(r_chunk.getFileName(), r_chunk.getIpaddress(),r_chunk.getPort());
        }

    }

    private void data_reply(String filename, String ipaddress, int port) throws InterruptedException {
        int i = 1;
        boolean finished = false;
        while(!finished)
        {
            Thread.sleep(1000);
            String key = filename + Integer.toString(i);
            finished = search_and_send(key,ipaddress,port);
            i += 1;

        }
    }

    private boolean search_and_send(String filekey, String ipaddress,int port){
        if(chunk_storage.containsKey(filekey)){
            send_to_node(filekey,ipaddress,port);
        }else
            request_from_storage(ipaddress,port);
        System.out.println("data chunk " + filekey + "sent");
        if(chunk_storage.get(filekey).getIs_last())
            return true;
        else
            return false;
    }

    private void send_to_node(String filekey,String ipaddress, int port){
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
        }

    private void request_from_storage(String ipaddress, int port){

    }



    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode(5050);
        storageNode.startNode();
    }



}
