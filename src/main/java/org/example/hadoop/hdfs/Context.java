package org.example.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    public Object read(Object key) {
        return cacheMap.get(key);
    }
}
