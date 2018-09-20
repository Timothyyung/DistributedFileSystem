package edu.usfca.cs.dfs.Coordinator.HashPackage;

import java.math.BigInteger;

public class HashRingEntry {
    public BigInteger position;
    public HashRingEntry neighbor;

    public HashRingEntry(BigInteger position)
    {
        this.position = position;
        this.neighbor = this;
    }

    public HashRingEntry(BigInteger position, HashRingEntry neighbor){
        this.position = position;
        this.neighbor = neighbor;
    }

}
