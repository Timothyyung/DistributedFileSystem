package edu.usfca.cs.dfs.DataSender;

import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.StorageMessages;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class DataRequesterCoord extends Thread{
    private CoordMessages.DataPacket dataPacket;
    private boolean sent;
    private String ipaddress;
    private int port;
    private static final Logger logger = LogManager.getRootLogger();
    public DataRequesterCoord(CoordMessages.DataPacket dataPacket, String ipaddress, int port){
        this.dataPacket = dataPacket;
        this.sent = false;
        this.ipaddress = ipaddress;
        this.port = port;
    }

    @Override
    public void run() {

        logger.debug("Packet sent to:  " + ipaddress +":"+Integer.toString(port));
        while(!sent){
            try(
                    Socket s = new Socket(this.ipaddress,this.port);
                    OutputStream outputStream = s.getOutputStream();
            ){
                dataPacket.writeDelimitedTo(outputStream);
                sent = true;
            }catch (IOException ie){
                ie.getStackTrace();
            }
        }
    }
}
