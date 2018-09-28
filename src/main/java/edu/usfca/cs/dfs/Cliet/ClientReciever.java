package edu.usfca.cs.dfs.Cliet;

import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.StorageMessages;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ClientReciever extends Thread {
    private boolean run;
    private ConcurrentHashMap<String, Chunk> chunkmap;
    public ClientReciever() {
        this.run = true;
        chunkmap = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        System.out.println("Very exited to recieve some Chunks");
        while (this.run){
            try(
                    ServerSocket serverSocket = new ServerSocket(1010);
                    Socket sock = serverSocket.accept();
            ){
                Client_reciever cr = new Client_reciever(sock);
                cr.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("The server is tired...");
    }


    private class Client_reciever extends Thread{
        private Socket s;
        public Client_reciever(Socket socket){
            this.s = socket;
        }

        @Override
        public void run() {
            try {
                InputStream instream = s.getInputStream();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.parseDelimitedFrom(instream);
                Chunk chunk = new Chunk(dataPacket);
                chunkmap.put(chunk.getFile_name() + Integer.toString(chunk.getChunk_id()),chunk);
                if(chunk.getIs_last()){

                }
                s.close();
            }catch(IOException e) { }
        }
    }

    private void reassemble(String filename)
    {
        List<Byte> file = new ArrayList<>();
        boolean finished = false;
        int i = 1;
        byte[] chunk;
        while(!finished)
        {
            String key = filename + Integer.toString(i);
            if(chunkmap.containsKey(key))
                chunk = chunkmap.get(key).getData_chunk();
            else{
                chunk = request_chunk(filename, i)
            }
            for(int j = 0; j < chunk.length ; j++)
                file.add(chunk[j]);

            i += 1;
            if(chunkmap.get(key).getIs_last()){
                finished = true;
            }
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

    private byte[] request_chunk(String filename, int chunkid){


    }


}
