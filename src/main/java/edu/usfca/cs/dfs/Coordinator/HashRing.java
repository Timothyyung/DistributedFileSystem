package edu.usfca.cs.dfs.Coordinator;

import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashFunction;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashRingEntry;
import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;

import java.math.BigInteger;
import java.util.TreeMap;


public class HashRing<T> {
    protected HashFunction<T> function;
    protected BigInteger maxHash;
    protected boolean randomize = false;

    protected TreeMap<BigInteger, HashRingEntry> entryMap = new TreeMap<>();

    public HashRing(HashFunction<T> function){
        this(function, false);
    }

    public HashRing(HashFunction<T> function, boolean randomize){
        this.function = function;
        this.randomize = randomize;
        maxHash = function.maxValue();
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
        return BigInteger.ZERO;
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
}


