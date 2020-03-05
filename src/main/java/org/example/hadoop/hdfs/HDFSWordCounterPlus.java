package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

// Word Counter in HDFS, output the result to the HDFS
public class HDFSWordCounterPlus {
    public static void main(String[] args) throws Exception {
        Properties properties = ParamsUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URL)), new Configuration());

        Context context = new Context();
        Map<Object, Object> hm = context.getCacheMap();
        Mapper mapper = new WordCountMapper();
        RemoteIterator<LocatedFileStatus> iter = fileSystem.listFiles(input, false);
        while (iter.hasNext()) {
            LocatedFileStatus file = iter.next();
            FSDataInputStream in = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null) {
                mapper.map(line, context);
            }
            reader.close();
            in.close();
        }

        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fileSystem.create(new Path(output, new Path(properties.getProperty(Constants.OUTPUT_FILE))));

        for (Map.Entry<Object, Object> entry : hm.entrySet()) {
            out.write((entry.getKey().toString() + " \t " + entry.getValue() + "\n").getBytes());
        }

        out.close();
        fileSystem.close();

        System.out.println("Close.....");
    }


}
