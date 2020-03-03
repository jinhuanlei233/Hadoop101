package org.example.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://localhost:9000";
    FileSystem fileSystem = null;
    Configuration config = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("-----SetUp-----");
        config = new Configuration();
        config.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), config);
    }

    @After
    public void tearDown() {
        fileSystem = null;
        config = null;
        System.out.println("-----TearDown-----");
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println("result: " + result);
    }

    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/test.rb"));
        IOUtils.copyBytes(in, System.out, 1024);
    }

    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        out.writeUTF("hello world jlei");
        out.flush();
        out.close();
    }

    @Test
    public void testReplica() {
        System.out.println(config.get("dfs.replication"));
    }

    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/b.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("/Users/jlei/Desktop/test.rb");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    // with large file, add a loading bar
    @Test
    public void copyFromLocalLargeFile() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/jlei/Documents/Hadoop/hadoop-2.6.0-cdh5.15.1.tar.gz")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/bigFile.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/c.txt");
        Path dst = new Path("/Users/jlei/Documents/Hadoop/");
        fileSystem.copyToLocalFile(src, dst);
    }

    @Test
    public void listFilesStatus() throws Exception {
        Path dst = new Path("/hdfsapi/test/");
        FileStatus[] fileStatuses = fileSystem.listStatus(dst);
        for (FileStatus file : fileStatuses) {
            String isDir = file.isDirectory() ? "Folder" : "File";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t"
                    + path);
        }
    }

    @Test
    public void listFilesRecursive() throws Exception {
        Path dst = new Path("/");
        RemoteIterator<LocatedFileStatus> iter = fileSystem.listFiles(dst, true);
        while (iter.hasNext()) {
            LocatedFileStatus file = iter.next();
            String isDir = file.isDirectory() ? "Folder" : "File";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long length = file.getLen();
            String path = file.getPath().toString();

            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + length + "\t"
                    + path);
        }
    }

    @Test
    public void getFileBlockLocations() throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/bigFile.tar.gz"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }
    }

    @Test
    public void delete() throws IOException {
        boolean result = fileSystem.delete(new Path("/hdfsapi/test/bigFile.tar.gz"));
        System.out.println(result);
    }

}
