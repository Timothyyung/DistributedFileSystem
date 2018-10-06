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
    private HashMap<String, BigInteger> node_map;
    boolean running = true;
    public Heartbeat (HashMap<String,BigInteger> node_map, int mapsize){
        this.mapsize = mapsize;
        this.node_map = node_map;
    }

    @Override
    public synchronized void start() {
        while(running) {
            try {
                Thread.sleep(5000);
                //are_u_alive();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private void are_u_alive(String ipaddress, int port) throws IOException {

        Socket s = new Socket(ipaddress,port);
        OutputStream outputStream = s.getOutputStream();
        InputStream inputStream = s.getInputStream();
        CoordMessages.Heartbeat heartbeat = CoordMessages.Heartbeat.newBuilder()
                .setMapSize(mapsize)
                .build();
        CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                .setHeartbeat(heartbeat)
                .build();
        dataPacket.writeDelimitedTo(outputStream);

        dataPacket = dataPacket.parseDelimitedFrom(inputStream);
        if(!dataPacket.hasHeartbeat())
            kill_node(ipaddress,port);

    }

    private void kill_node(String ipadress, int port)
    {
        
    }

    public void set_map_size(int mapsize)
    {
        this.mapsize = mapsize;
    }

}
