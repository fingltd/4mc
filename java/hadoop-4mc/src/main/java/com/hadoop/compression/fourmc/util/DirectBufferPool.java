package com.hadoop.compression.fourmc.util;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Pool for direct buffers, used by compressors and decompressors.
 */
public class DirectBufferPool {
    private static DirectBufferPool instance;

    static {
        instance = new DirectBufferPool();
    }

    public static DirectBufferPool getInstance() {
        return instance;
    }

    // -------------------------------------------------------------------------------

    public static class Stats {
        public long allocatedBuffers = 0;
        public long usedBuffers = 0;
        public long allocatedBytes = 0;
        public long usedBytes = 0;

        public Stats() {
        }
        public Stats(long allocatedBuffers, long usedBuffers, long allocatedBytes, long usedBytes) {
            this.allocatedBuffers = allocatedBuffers;
            this.usedBuffers = usedBuffers;
            this.allocatedBytes = allocatedBytes;
            this.usedBytes = usedBytes;
        }
    }

    // -------------------------------------------------------------------------------

    private HashMap<Integer, List<ByteBuffer> > poolMap;
    private boolean enabled = true;
    private long allocatedBuffers = 0;
    private long usedBuffers = 0;
    private long allocatedBytes = 0;
    private long usedBytes = 0;

    private DirectBufferPool() {
        poolMap = new HashMap<Integer, List<ByteBuffer> >();
    }

    public synchronized void enable() {
        if (enabled) return;
        enabled=true;
        if (poolMap==null) {
            poolMap = new HashMap<Integer, List<ByteBuffer> >();
        }
    }

    public synchronized void forceReleaseBuffers() {
        if (!enabled) return;
        allocatedBuffers=0;
        usedBuffers=0;
        allocatedBytes=0;
        usedBytes=0;
        poolMap.clear();
    }

    public synchronized void disable() {
        enabled=false;
        allocatedBuffers=0;
        usedBuffers=0;
        allocatedBytes=0;
        usedBytes=0;
        poolMap.clear();
        poolMap=null;
    }

    public synchronized boolean isEnabled() {
        return enabled;
    }

    public synchronized Stats getStats() {
        return new Stats(allocatedBuffers, usedBuffers, allocatedBytes, usedBytes);
    }

    public synchronized ByteBuffer allocate(int capacity) {
        if (!enabled) return ByteBuffer.allocateDirect(capacity);
        List<ByteBuffer> pool = poolMap.get(capacity);
        if (pool==null || pool.isEmpty()) {
            ++allocatedBuffers;
            allocatedBytes+=capacity;
            ++usedBuffers;
            usedBytes+=capacity;
            return ByteBuffer.allocateDirect(capacity);
        }
        ByteBuffer res = pool.get(0);
        pool.remove(0);
        ++usedBuffers;
        usedBytes+=res.capacity();
        return res;
    }

    public synchronized void release(ByteBuffer buff) {
        if (!enabled) return;
        List<ByteBuffer> pool = poolMap.get(buff.capacity());
        if (pool==null) {
            pool = new LinkedList<ByteBuffer>();
            poolMap.put(buff.capacity(), pool);
        }
        buff.clear();
        pool.add(buff);
        --usedBuffers;
        usedBytes-=buff.capacity();
    }

}
