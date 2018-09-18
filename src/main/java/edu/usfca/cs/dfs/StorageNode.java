package edu.usfca.cs.dfs;

import edu.usfca.cs.dfs.Data.Chunk;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class StorageNode extends Thread{

    private HashMap<String,Chunk> chunk_storage = new HashMap<>();
    private int hashspace;
    private ReentrantLock lock = new ReentrantLock();

    public static void main(String[] args) 
    throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        StorageNode storageNode = new StorageNode();
        storageNode.startNode();
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


    public synchronized void store_chunk(Chunk chunk)
    {
        boolean isLocked = lock.tryLock();
        while(!isLocked)
        {
            try{
                isLocked = lock.tryLock(1, TimeUnit.SECONDS);
            }catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        try{
            chunk_storage.put(chunk.get_hash_key(),chunk);
        }finally {
            lock.unlock();
        }
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
                ServerSocket serverSocket = new ServerSocket(5050);
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
                StorageMessages.StoreChunk r_chunk = StorageMessages.StoreChunk.parseDelimitedFrom(instream);
                Chunk s_chunk = new Chunk(r_chunk.getData().toByteArray(),r_chunk.getFileName(),r_chunk.getChunkId());
                store_chunk(s_chunk);
                System.out.println(s_chunk.getChunk_id());
                System.out.println(s_chunk.getFile_name());
                System.out.println(s_chunk.getData_chunk().length);
                s.close();
            }catch(IOException e)
            {

            }
        }
    }



}
