package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.DataSender.DataRequester;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

public class NodeTimer extends Thread {
    private boolean alive;
    private BigInteger pos;
    private Timer timer;
    private int time;
    private String key;
    private String coordIp;
    private int coordport;



    public NodeTimer(BigInteger pos, String key,String coordIp, int coordport){
        this.alive = true;
        this.pos = pos;
        this.timer = new Timer();
        this.time = 5;
        this.key = key;
        this.coordIp = coordIp;
        this.coordport = coordport;
    }

    @Override
    public void start() {

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                boolean sent = false;
                time--;
                if(time == 0){

                    while(!sent){
                        try(
                                Socket s = new Socket(coordIp,coordport);
                                OutputStream outputStream = s.getOutputStream();
                        ){
                            CoordMessages.RemoveNode removeNode = CoordMessages.RemoveNode.newBuilder()
                                    .setKey(key)
                                    .build();
                            CoordMessages.DataPacket dataPacket = CoordMessages.DataPacket.newBuilder()
                                    .setRemovenode(removeNode)
                                    .build();
                            dataPacket.writeDelimitedTo(outputStream);
                            sent = true;
                        }catch (IOException ie){
                            ie.getStackTrace();
                        }
                    }
                }
            }
        }, 0, 1000);
    }

    public boolean getAlive(){
        return alive;
    }

    public void resetTime(){
        this.time = 5;
        alive = true;
    }

    public BigInteger getPos(){
        return pos;
    }
}
