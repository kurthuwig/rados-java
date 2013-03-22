package com.ceph;

public class RadosException extends Exception {

    protected int returnValue;

    public RadosException(String message) {
        super(message);
    }

    public RadosException(String message, int returnValue) {
        super(message);
        this.returnValue = returnValue;
    }

    public int getReturnValue() {
        return this.returnValue;
    }

}
