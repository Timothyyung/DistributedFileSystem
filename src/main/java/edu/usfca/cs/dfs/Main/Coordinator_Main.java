package edu.usfca.cs.dfs.Main;

import edu.usfca.cs.dfs.Coordinator.Coordinator;
import edu.usfca.cs.dfs.Storage.StorageNode;

public class Coordinator_Main {
    private int port;
    public Coordinator_Main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-port")) {
                i++;
                this.port = Integer.parseInt(args[i]);
            }
        }

    }
    public static void main(String[] args) {

        Coordinator_Main smain = new Coordinator_Main(args);
        System.out.println(smain.port);
        Coordinator coordinator = new Coordinator(smain.port);
        coordinator.outputInfo();
        coordinator.startCoord();
    }
}
