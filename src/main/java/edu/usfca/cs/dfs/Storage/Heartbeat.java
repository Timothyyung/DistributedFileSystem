package edu.usfca.cs.dfs.Storage;

import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.DataSender.DataRequester;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.HashMap;

public class Heartbeat extends Thread{
    private int mapsize;
    boolean running = true;
    private String key;
    private String coordip;
    private int coordport;
    public Heartbeat (int mapsize, String key,String coordip,int coordport){
        this.mapsize = mapsize;
        this.key = key;
        this.coordip = coordip;
        this.coordport = coordport;
    }

    @Override
    public synchronized void run() {
        while(running) {
            try {
                i_am_alive(coordip,coordport);
                Thread.sleep(1500);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }

        }
    }

    private void i_am_alive(String ipaddress, int port) throws IOException {

        Socket s = new Socket(ipaddress,port);
        OutputStream outputStream = s.getOutputStream();
        InputStream inputStream = s.getInputStream();
        CoordMessages.Heartbeat heartbeat = CoordMessages.Heartbeat.newBuilder()
                .setMapSize(mapsize)
                .setNodeKey(key)
                .build();
        CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                .setHeartbeat(heartbeat)
                .build();
        dataPacket.writeDelimitedTo(outputStream);



    }



    public void set_map_size(int mapsize)
    {
        this.mapsize = mapsize;
    }

}
