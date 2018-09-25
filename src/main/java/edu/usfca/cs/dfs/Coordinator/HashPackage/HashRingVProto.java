package edu.usfca.cs.dfs.Coordinator.HashPackage;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages.HashRing;
import edu.usfca.cs.dfs.CoordMessages.HashRingEntry;

import java.math.BigInteger;


public class HashRingVProto<T> {
/*
    protected HashFunction<T> function;
    protected BigInteger maxHash;
    protected boolean randomize;

    protected HashRing entryMap = HashRing.getDefaultInstance();

    public HashRingVProto(HashFunction<T> function){
        this(function, false);
    }

    public HashRingVProto(HashFunction<T> function, boolean randomize){
        this.function = function;
        this.randomize = randomize;
        maxHash = function.maxValue();
    }
    private void addRingEntry(BigInteger position, HashRingEntry predecessor, String ipaddress, int port) throws HashTopologyException{
        if (entryMap.getHashRings().get(position.toString()) != null){
            System.out.println(position);
            throw new HashTopologyException("Hash space exhausted!");
        }
        ByteString bsval = ByteString.copyFrom(position.toByteArray(), 0, position.toByteArray().length);
        HashRingEntry newEntry = HashRingEntry.newBuilder()
                .setPosition(bsval)
                .setNeighbor(predecessor.getNeighbor())
                .setIpaddress(ipaddress)
                .setPort(port)
                .build();
        predecessor = HashRingEntry.newBuilder()
                .setPosition(predecessor.getPosition())
                .setNeighbor(newEntry)
                .setIpaddress(predecessor.getIpaddress())
                .setPort(predecessor.getPort())
                .build();
        entryMap.getHashRings().put(predecessor.getPosition().toString(),predecessor);
        entryMap.getHashRings().put(position.toString(),newEntry);
    }



    public BigInteger addNode(String ipaddress, int port) throws HashTopologyException, HashException{
        if (entryMap.getHashRings().values().size() == 0) {
            BigInteger pos;

            if (randomize) {
                /* Find a random location to start with */
/*
                pos = function.randomHash();
            } else {
                pos = BigInteger.ZERO;
            }
            ByteString bsval = ByteString.copyFrom(pos.toByteArray(), 0, pos.toByteArray().length);
            HashRingEntry firstEntry = HashRingEntry.getDefaultInstance();
            firstEntry = HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setIpaddress(ipaddress)
                    .setPort(port)
                    .setNeighbor(firstEntry)
                    .build();
            entryMap.getHashRings().put(pos.toString(), firstEntry);

            return pos;
        }

        if(entryMap.getHashRings().values().size() == 1) {
            HashRingEntry firstEntry = entryMap.getHashRings().values().iterator().next();
            BigInteger halfSize = maxHash.divide(BigInteger.valueOf(2));
            BigInteger secondPos = new BigInteger(firstEntry.getPosition().toByteArray()).add(halfSize);
            if (secondPos.compareTo(maxHash) > 0) {
                secondPos = secondPos.subtract(maxHash);
            }
            ByteString bsval = ByteString.copyFrom(secondPos.toByteArray(), 0, secondPos.toByteArray().length);
            HashRingEntry secondEntry = HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setIpaddress(ipaddress)
                    .setPort(port)
                    .setNeighbor(firstEntry)
                    .build();
            firstEntry = HashRingEntry.newBuilder()
                    .setPosition(bsval)
                    .setIpaddress(ipaddress)
                    .setPort(port)
                    .setNeighbor(secondEntry)
                    .build();



            entryMap.getHashRings().put(secondPos.toString(), secondEntry);
            entryMap.getHashRings().put(firstEntry.getPosition().toString(), firstEntry);
            return secondPos;
        }

        BigInteger largestSpan = BigInteger.ZERO;
        HashRingEntry largestEntry = null;
        for (HashRingEntry entry : entryMap.getHashRings().values()){
            BigInteger len = lengthBetween(entry,entry.getNeighbor());
            if(len.compareTo(largestSpan) > 0){
                largestSpan = len;
                largestEntry = entry;
            }
        }

        if(largestEntry == null){
            return  BigInteger.ONE.negate();
        }

        BigInteger half = half(largestEntry,largestEntry.getNeighbor());
        addRingEntry(half,largestEntry,ipaddress,port);
        return half;
    }


    private BigInteger half(HashRingEntry start, HashRingEntry end){
        BigInteger length = lengthBetween(start, end);
        BigInteger half = new BigInteger(start.getPosition().toByteArray()).add(length.divide(BigInteger.valueOf(2)));

        if(maxHash.compareTo(half) >= 0) {
            return half;
        } else {
            return half.subtract(maxHash);
        }
    }

    private BigInteger lengthBetween(HashRingEntry start, HashRingEntry end){
        BigInteger difference = new BigInteger(end.getPosition().toByteArray()).subtract(new BigInteger(start.getPosition().toByteArray()));
        if (difference.compareTo(BigInteger.ZERO) >= 0){
            return difference;
        } else {
            BigInteger wrapped = maxHash.subtract(new BigInteger(start.getPosition().toByteArray())).add(new BigInteger(end.getPosition().toByteArray()));
            return wrapped;
        }
    }

    public BigInteger locate(T data) throws HashException {
        BigInteger hashLocation = function.hash(data);
        BigInteger node = entryMap.getHashRings().ceilingKey(hashLocation);
        if(node == null){
            node = entryMap.ceilingKey(BigInteger.ZERO);
        }
        return node;
    }

    public void return_entries()
    {
        System.out.println(entryMap.toString());
    }

    @Override
    public String toString(){
        String str = "";
        HashRingEntry firstEntry = entryMap.values().iterator().next();
        HashRingEntry currentEntry = firstEntry;
        HashRingEntry nextEntry;

        do {
            nextEntry = currentEntry.neighbor;
            str += currentEntry.position + " - > " + nextEntry.position;
            str += System.lineSeparator();
            currentEntry = nextEntry;
        }while (currentEntry != firstEntry);
        return str;
        }
        */
}


