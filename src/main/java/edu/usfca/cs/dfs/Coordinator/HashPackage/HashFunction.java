package edu.usfca.cs.dfs.Coordinator.HashPackage;

import java.math.BigInteger;

public interface HashFunction<T> {

    public BigInteger hash(T data) throws HashException;

    public BigInteger maxValue();

    public BigInteger randomHash() throws HashException;
}
