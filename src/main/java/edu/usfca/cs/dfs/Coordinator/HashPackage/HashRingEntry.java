package edu.usfca.cs.dfs.Coordinator.HashPackage;

import java.math.BigInteger;

public class HashRingEntry {
    public BigInteger position;
    public HashRingEntry neighbor;
    public String inetaddress;
    public int port;

    public HashRingEntry(BigInteger position)
    {
        this.position = position;
        this.neighbor = this;
    }

    public HashRingEntry(BigInteger position, String inetaddress, int port)
    {
        this.position = position;
        this.inetaddress = inetaddress;
        this.port = port;
    }

    public HashRingEntry(BigInteger position, HashRingEntry neighbor){
        this.position = position;
        this.neighbor = neighbor;
    }

    public HashRingEntry(BigInteger position, HashRingEntry neighbor, String inetaddress, int port)
    {
        this.position = position;
        this.neighbor = neighbor;
        this.inetaddress = inetaddress;
        this.port = port;
    }

}
