package edu.usfca.cs.dfs.Coordinator;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.CoordMessages;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashFunction;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.DataSender.DataRequester;
import edu.usfca.cs.dfs.StorageMessages;

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

    public HashRing(HashFunction<T> function, CoordMessages.HashRing map)
    {
        this(function,false);
        map_to_treemap(map);
        remap_hashring();
    }

    public HashRingEntry returnNode(BigInteger pos){
        return entryMap.get(pos);
    }

    private void map_to_treemap(CoordMessages.HashRing map)
    {
        System.out.println("MAKING MAP TO TREEMAP");
        for(Map.Entry<String, CoordMessages.HashRingEntry> entry: map.getHashRings().entrySet()){
            BigInteger pos = new BigInteger(entry.getValue().getPosition().toByteArray());
            BigInteger key = new BigInteger(entry.getKey());
            HashRingEntry hashRingEntry = new HashRingEntry(pos,entry.getValue().getIpaddress(),entry.getValue().getPort());
            entryMap.put(key,hashRingEntry);
        }
    }

    public CoordMessages.HashRing treemap_to_map()
    {
        Map<String,CoordMessages.HashRingEntry> hashRing = new TreeMap<>();

        for (Map.Entry<BigInteger,HashRingEntry> entry: entryMap.entrySet()){
            String key = entry.getKey().toString();
            ByteString pos = ByteString.copyFrom(entry.getValue().position.toByteArray(), 0, entry.getValue().position.toByteArray().length);
            CoordMessages.HashRingEntry coord_hre = CoordMessages.HashRingEntry.newBuilder()
                    .setPosition(pos)
                    .setIpaddress(entry.getValue().inetaddress)
                    .setPort(entry.getValue().port)
                    .build();
            hashRing.put(key,coord_hre);
        }
        return CoordMessages.HashRing.newBuilder().putAllHashRings(hashRing).build();

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

    public void addNodePos(BigInteger position, String ipaddress,int port) throws HashTopologyException{
        if (entryMap.get(position) != null){
            System.out.println(position);
            throw new HashTopologyException("Hash space exhausted! or somthing is here already");
        }
        HashRingEntry predecessor;
        BigInteger node = entryMap.ceilingKey(position);
        if(node == null)
            predecessor = entryMap.get(BigInteger.ZERO);
        else
            predecessor = entryMap.get(node);

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
                //System.out.println("mapping " + prevEntry.position + " to " + hashRingEntry.position + "\n\n");
                prevEntry.neighbor = hashRingEntry;
                prevEntry = hashRingEntry;
            }

        }
        prevEntry.neighbor = firstEntry;
    }

    public void sendUpdate(String ipaddress, int port,CoordMessages.HashRingEntry hre){
        for(Map.Entry<BigInteger,HashRingEntry> entry: entryMap.entrySet()) {
            if(entry.getValue().inetaddress != ipaddress && entry.getValue().port != port){
                StorageMessages.HashRingEntry hashRingEntry = StorageMessages.HashRingEntry.newBuilder()
                        .setPosition(hre.getPosition())
                        .setIpaddress(hre.getIpaddress())
                        .setPort(hre.getPort())
                        .build();
                StorageMessages.DataPacket dataPacket = StorageMessages.DataPacket.newBuilder()
                        .setHashringentry(hashRingEntry)
                        .build();
                DataRequester dataRequester = new DataRequester(dataPacket,entry.getValue().inetaddress,entry.getValue().port);
                dataRequester.start();
            }

        }
    }

}


