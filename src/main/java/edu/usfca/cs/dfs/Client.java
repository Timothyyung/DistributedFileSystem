package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.Data.Chunk;
import edu.usfca.cs.dfs.Data.Data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Client {

    public static void main(String[] args) {

        byte[] a = new byte[100];
        Data data = new Data("inputs/Mytestdoc.txt");
        System.out.println(data.getData().length);
        Chunk chunk = new Chunk(data.getData(),"a.txt",1);
        Client client = new Client();
        client.send("localhost", 5050, chunk);
    }
    public void send(String ipaddress,int port, Chunk chunk){
        try(
            Socket s = new Socket(ipaddress,port);
            OutputStream outputStream = s.getOutputStream();
            InputStream inputStream = s.getInputStream();
        ){
            ByteString bsval = ByteString.copyFrom(chunk.getData_chunk(),0,chunk.getData_chunk().length);
            StorageMessages.StoreChunk s_chunk = StorageMessages.StoreChunk.newBuilder()
                    .setChunkId(chunk.getChunk_id())
                    .setData(bsval)
                    .setFileName(chunk.getFile_name())
                    .build();
            s_chunk.writeDelimitedTo(outputStream);

       }catch(IOException e){
            e.printStackTrace();
        }
   }
}