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
    public Heartbeat (HashMap<String,BigInteger> node_map, int mapsize){
        this.mapsize = mapsize;
    }

    @Override
    public synchronized void start() {
        while(running) {
            try {
                Thread.sleep(5000);
                i_am_alive("localhost",6000);
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
                .setNodeKey(ipaddress +Integer.toString(port))
                .build();
        CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                .setHeartbeat(heartbeat)
                .build();
        dataPacket.writeDelimitedTo(outputStream);

        dataPacket = dataPacket.parseDelimitedFrom(inputStream);
        heartbeat = dataPacket.getHeartbeat();
        heartbeat.getMapSize();


    }



    public void set_map_size(int mapsize)
    {
        this.mapsize = mapsize;
    }

}
