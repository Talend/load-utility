package com.thoughtspot.load_utility;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TSReader {
    private boolean isCompleted = false;
    private ConcurrentLinkedQueue<String> records = new ConcurrentLinkedQueue<String>();
    private static TSReader instance = null;

    public static synchronized TSReader newInstance()
    {
        if (instance == null)
            instance = new TSReader();

        return instance;
    }

    private TSReader() {
    }

    public synchronized void setIsCompleted(boolean isCompleted)
    {
        this.isCompleted = isCompleted;
    }

    public synchronized boolean getIsCompleted()
    {
        return this.isCompleted;
    }

    public synchronized boolean add(String record)
    {
        return records.add(record);
    }

    public synchronized String poll()
    {
        return records.poll();
    }

}
