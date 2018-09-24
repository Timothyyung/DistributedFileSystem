package edu.usfca.cs.dfs.Playground;

import edu.usfca.cs.dfs.Coordinator.HashPackage.HashException;

import edu.usfca.cs.dfs.Coordinator.HashPackage.HashTopologyException;
import edu.usfca.cs.dfs.Coordinator.HashPackage.SHA1;
import edu.usfca.cs.dfs.Data.Data;
import edu.usfca.cs.dfs.Coordinator.*;

import java.math.BigInteger;


public class datatest {
    public static void main(String[] args) throws HashException, HashTopologyException {
        SHA1 sha1 = new SHA1();
        System.out.println(sha1.maxValue());
        Data data = new Data("inputs/Mytestdoc.txt");
        BigInteger bigInteger;
        System.out.println(sha1.hash("abc".getBytes()));
        System.out.println(sha1.hash("abc".getBytes()));
        System.out.println(sha1.hash("abc".getBytes()));
        System.out.println(sha1.hash("abc".getBytes()));

        try {
            bigInteger = sha1.hash(data.getData());
            bigInteger.toByteArray();
            BigInteger bigInteger2 = new BigInteger(data.getData());
            System.out.println("data as a big int " + bigInteger2);
        } catch (HashException e) {
            e.printStackTrace();
        }
        HashRing<byte[]> hashRing = new HashRing(sha1);

        BigInteger abc =hashRing.addNode("abc".getBytes());
        BigInteger dce = hashRing.addNode("dce".getBytes());
        BigInteger jello = hashRing.addNode("jello".getBytes());
        BigInteger cookie = hashRing.addNode("cookie".getBytes());



        System.out.println(hashRing.toString());
        System.out.println(hashRing.locate(("a".getBytes())));
        System.out.println(hashRing.locate(("b".getBytes())));
        System.out.println(hashRing.locate(("c".getBytes())));
        System.out.println(hashRing.locate(("d".getBytes())));
        System.out.print("\n\n\n\n");
        System.out.println(hashRing.getRingEntry(jello).position);
        hashRing.return_entries();

    }
};

