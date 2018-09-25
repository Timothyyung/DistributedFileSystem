package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashFunction;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;


public class HashRing<T> {
    protected HashFunction<T> function;
    protected BigInteger maxHash;
    protected boolean randomize;

    protected TreeMap<BigInteger, HashRingEntry> entryMap = new TreeMap<>();

    public HashRing(HashFunction<T> function){
        this(function, false);
    }

    public HashRing(HashFunction<T> function, boolean randomize){
        this.function = function;
        this.randomize = randomize;
        maxHash = function.maxValue();
    }

    public HashRing(HashFunction<T> function, Map<String,CoordMessages.HashRingEntry> map)
    {
        this(function,false);
        map_to_treemap(map);
        remap_hashring();
    }

    private void map_to_treemap(Map<String,CoordMessages.HashRingEntry> map)
    {
        for(Map.Entry<String, CoordMessages.HashRingEntry> entry: map.entrySet()){
            BigInteger pos = new BigInteger(entry.getValue().getPosition().toByteArray());
            BigInteger key = new BigInteger(entry.getKey().getBytes());
            HashRingEntry hashRingEntry = new HashRingEntry(pos,entry.getValue().getIpaddress(),entry.getValue().getPort());
            entryMap.put(key,hashRingEntry);
        }
    }

    private void addRingEntry(BigInteger position, HashRingEntry predecessor) throws HashTopologyException{
        if (entryMap.get(position) != null){
            System.out.println(position);
            throw new HashTopologyException("Hash space exhausted!");
        }

        HashRingEntry newEntry = new HashRingEntry(position,predecessor.neighbor);
        predecessor.neighbor = newEntry;
        entryMap.put(position,newEntry);
    }

    private void addRingEntry(BigInteger position, HashRingEntry predecessor,String ipaddress, int port) throws HashTopologyException{
        if (entryMap.get(position) != null){
            System.out.println(position);
            throw new HashTopologyException("Hash space exhausted!");
        }

        HashRingEntry newEntry = new HashRingEntry(position,predecessor.neighbor,ipaddress,port);
        predecessor.neighbor = newEntry;
        entryMap.put(position,newEntry);
    }

    public TreeMap<BigInteger, HashRingEntry> getMap()
    {
        return entryMap;
    }
    public BigInteger addNode(String ipaddress, int port) throws HashTopologyException, HashException{
        if (entryMap.values().size() == 0) {
            BigInteger pos;

            if (randomize) {
                /* Find a random location to start with */
                pos = function.randomHash();
            } else {
                pos = BigInteger.ZERO;
            }

            HashRingEntry firstEntry = new HashRingEntry(pos,ipaddress,port);
            entryMap.put(pos, firstEntry);

            return pos;
        }

        if(entryMap.values().size() == 1) {
            HashRingEntry firstEntry = entryMap.values().iterator().next();
            BigInteger halfSize = maxHash.divide(BigInteger.valueOf(2));
            BigInteger secondPos = firstEntry.position.add(halfSize);
            if (secondPos.compareTo(maxHash) > 0) {
                secondPos = secondPos.subtract(maxHash);
            }
            HashRingEntry secondEntry = new HashRingEntry(secondPos, firstEntry,ipaddress,port);
            firstEntry.neighbor = secondEntry;
            entryMap.put(secondPos, secondEntry);
            return secondPos;
        }

        BigInteger largestSpan = BigInteger.ZERO;
        HashRingEntry largestEntry = null;
        for (HashRingEntry entry : entryMap.values()){
            BigInteger len = lengthBetween(entry,entry.neighbor);
            if(len.compareTo(largestSpan) > 0){
                largestSpan = len;
                largestEntry = entry;
            }
        }

        if(largestEntry == null){
            return  BigInteger.ONE.negate();
        }

        BigInteger half = half(largestEntry,largestEntry.neighbor);
        addRingEntry(half,largestEntry,ipaddress,port);
        return half;
    }


    public BigInteger addNode(T data) throws HashTopologyException, HashException{
        if (entryMap.values().size() == 0) {
            BigInteger pos;

            if (randomize) {
                /* Find a random location to start with */
                pos = function.randomHash();
            } else {
                pos = BigInteger.ZERO;
            }

            HashRingEntry firstEntry = new HashRingEntry(pos);
            entryMap.put(pos, firstEntry);

            return pos;
        }

        if(entryMap.values().size() == 1) {
            HashRingEntry firstEntry = entryMap.values().iterator().next();
            BigInteger halfSize = maxHash.divide(BigInteger.valueOf(2));
            BigInteger secondPos = firstEntry.position.add(halfSize);
            if (secondPos.compareTo(maxHash) > 0) {
                secondPos = secondPos.subtract(maxHash);
            }
            HashRingEntry secondEntry = new HashRingEntry(secondPos, firstEntry);
            firstEntry.neighbor = secondEntry;
            entryMap.put(secondPos, secondEntry);
            return secondPos;
        }

        BigInteger largestSpan = BigInteger.ZERO;
        HashRingEntry largestEntry = null;
        for (HashRingEntry entry : entryMap.values()){
            BigInteger len = lengthBetween(entry,entry.neighbor);
            if(len.compareTo(largestSpan) > 0){
                largestSpan = len;
                largestEntry = entry;
            }
        }

        if(largestEntry == null){
            return  BigInteger.ONE.negate();
        }

        BigInteger half = half(largestEntry,largestEntry.neighbor);
        addRingEntry(half,largestEntry);
        return half;
    }

    private BigInteger half(HashRingEntry start, HashRingEntry end){
        BigInteger length = lengthBetween(start, end);
        BigInteger half = start.position.add(length.divide(BigInteger.valueOf(2)));

        if(maxHash.compareTo(half) >= 0) {
            return half;
        } else {
            return half.subtract(maxHash);
        }
    }

    private BigInteger lengthBetween(HashRingEntry start, HashRingEntry end){
        BigInteger difference = end.position.subtract(start.position);
        if (difference.compareTo(BigInteger.ZERO) >= 0){
            return difference;
        } else {
            BigInteger wrapped = maxHash.subtract(start.position).add(end.position);
            return wrapped;
        }
    }

    public BigInteger locate(T data) throws HashException {
        BigInteger hashLocation = function.hash(data);
        BigInteger node = entryMap.ceilingKey(hashLocation);
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

    public void unneighbor()
    {
        for (Map.Entry<BigInteger, HashRingEntry> entry : entryMap.entrySet()) {
            BigInteger pos = entry.getKey();
            HashRingEntry hashRingEntry = entry.getValue();
            hashRingEntry.neighbor = hashRingEntry;

        }
    }

    public void remap_hashring() {
        HashRingEntry firstEntry = entryMap.values().iterator().next();
        HashRingEntry currentEntry = firstEntry;
        HashRingEntry prevEntry = firstEntry;

        for (Map.Entry<BigInteger, HashRingEntry> entry : entryMap.entrySet()) {
            HashRingEntry hashRingEntry = entry.getValue();
            if (hashRingEntry != firstEntry) {
                System.out.println("mapping " + prevEntry.position + " to " + hashRingEntry.position + "\n\n");
                prevEntry.neighbor = hashRingEntry;
                prevEntry = hashRingEntry;
            }

        }
        prevEntry.neighbor = firstEntry;
    }
}


