package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Coordinator.HashRing;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;


public class StorageNode extends Thread{

    private ConcurrentHashMap<String,Chunk> chunk_storage = new ConcurrentHashMap<>();
    //private HashMap<String,Chunk> chunk_storage = new HashMap<>();
    private BigInteger hashpos;
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
        boolean run = true;
        System.out.println("Server Started");
        while (run){
            try(
                ServerSocket serverSocket = new ServerSocket(port);
                Socket sock = serverSocket.accept();
            ){
                store_chunk_listener scl = new store_chunk_listener(sock);
                scl.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class store_chunk_listener extends Thread{
        private Socket s;
        public store_chunk_listener(Socket s){
            this.s = s;
        }

        @Override
        public void run() {
            try {
                InputStream instream = s.getInputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);
                if(dataPacket.hasRequest())
                    process_request(dataPacket.getRequest());

                s.close();
            }catch(IOException e) { }
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
                    .setPort(port)
                    .build();
            requestEntry.writeDelimitedTo(outputStream);
            CoordMessages.Response response = CoordMessages.Response.getDefaultInstance();
            response = response.parseDelimitedFrom(inputStream);
            if(!response.getAllowed())
                System.out.println(("access denied"));
            else {

            }



        }catch (IOException ioe){
            ioe.printStackTrace();
        }

        return false;
    }




    private void process_request(StorageMessages.Request r_chunk){
        Chunk s_chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId());
        if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.store_chunk) {
            chunk_storage.put(s_chunk.get_hash_key(),s_chunk);
            System.out.println(s_chunk.getChunk_id());
            System.out.println(s_chunk.getFile_name());
            System.out.println(s_chunk.getData_chunk().length);
        }
        if(r_chunk.getOpcode() == StorageMessages.Request.Op_code.get_chunk) {
            System.out.println("total chunks" + get_total_chunks(r_chunk.getFileName()));
            reassemble(r_chunk.getFileName());
        }
    }

    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode();
        storageNode.startNode();
    }



}
