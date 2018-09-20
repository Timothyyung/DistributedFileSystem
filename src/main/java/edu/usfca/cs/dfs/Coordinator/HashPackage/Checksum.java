package edu.usfca.cs.dfs.Coordinator.HashPackage;

import com.sun.tools.javac.comp.Check;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

public class Checksum {
    private MessageDigest md;


    public Checksum(){
        try{
            md = MessageDigest.getInstance("SHA1");
        }catch (NoSuchAlgorithmException e){

        }
    }

    public Checksum(String algorithm) throws NoSuchAlgorithmException{
        md = MessageDigest.getInstance(algorithm);
    }

    public byte[] hash(byte[] bytes)
    {
        return md.digest(bytes);
    }

    public String hashToHexString(byte[] hash)
    {
        BigInteger bigInt = new BigInteger(1,hash);
        long targetLen = md.getDigestLength() * 2;
        return String.format("%0" + targetLen + "x", bigInt);
    }

    public MessageDigest getMessageDigest(){
        return md;
    }
}
