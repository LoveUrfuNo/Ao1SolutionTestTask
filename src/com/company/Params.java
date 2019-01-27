package com.company;

public class Params {
    private String dirPath;
    private int nThreads;
    private int waitingTime;

    public String getDirPath() {
        return dirPath;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    public int getnThreads() {
        return nThreads;
    }

    public void setnThreads(int nThreads) {
        this.nThreads = nThreads;
    }

    public int getWaitingTime() {
        return waitingTime;
    }

    public void setWaitingTime(int minutes) {
        this.waitingTime = minutes;
    }
}
