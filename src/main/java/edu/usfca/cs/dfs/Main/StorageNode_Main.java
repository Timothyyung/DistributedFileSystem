package edu.usfca.cs.dfs.Main;

import edu.usfca.cs.dfs.Storage.StorageNode;

import javax.swing.plaf.synth.SynthTabbedPaneUI;
import javax.swing.plaf.synth.SynthUI;

public class StorageNode_Main {

    private int port;
    private int cport;
    private String cip;
    public StorageNode_Main(String[] args)
    {
        for(int i = 0; i < args.length;i++){
            System.out.println(args[i]);
            if(args[i].equals("-port")){
                i++;
                this.port = Integer.parseInt(args[i]);
            }if(args[i].equals("-cip")){
                i++;
                this.cip = args[i];
            }if(args[i].equals("-cport")){
                i++;
                this.cport = Integer.parseInt(args[i]);
            }
        }
    }

    @Override
    public String toString() {
        return cip;
    }

    public static void main(String[] args) {

        StorageNode_Main smain = new StorageNode_Main(args);
        System.out.println(smain.cip);
        System.out.println(smain.cport);
        System.out.println(smain.port);
        StorageNode storageNode = new StorageNode(smain.port,smain.cip,smain.cport);
        System.out.println("Starting node on " + storageNode.toString());
        storageNode.startNode();
    }
}
