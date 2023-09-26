package org.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;

public class HDFSApp {
    Configuration configuration = null;
    FileSystem fileSystem = null;
    // 配置路径（ip地址）
    public static final String HDFS_PATH = "hdfs://172.21.0.4:8020";
    // 测试（新建文件夹）
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/createdbyjava1"));
    }
    @Test
    public void testDownFileToLocal() throws IOException {
        // 待下载的路径(HDFS)
        Path src = new Path("/testupload.txt");
        // 下载成功之后存放的路径(windows)
        Path dst = new Path("test.txt");
        // 下载
        fileSystem.copyToLocalFile(false,src,dst,true);
        System.out.println("下载成功");
    }
    // 上传文件
    @Test
    public void testUploadFileToHDFS() throws IOException {
        System.out.println("上传前文件列表如：");
        testCheckFile();
        Path src = new Path("testupload.txt");
        // 上传之后存放的路径(HDFS)
        Path dst = new Path("/testupload.txt");
        // 上传
        fileSystem.copyFromLocalFile(src,dst);
        System.out.println("上传成功后文件列表如：");
        testCheckFile();
    }

    @Test
    public void delDir() throws IOException, IOException {
        // 获取迭代器对象("/"表示获取全部目录下的文件)
        fileSystem.delete(new Path("/createdbyjava"));
    }
    @Test
    public void testCheckFile() throws IOException, IOException {
        // 获取迭代器对象("/"表示获取全部目录下的文件)
        FileStatus[] fs = fileSystem.listStatus(new Path("/"));
        Path[] listPath = FileUtil.stat2Paths(fs);
        for(Path p : listPath)
            System.out.println(p);
    }
    // Java 连接hdfs 需要先建立一个连接
    // 测试方法执行之前要执行的操作
    @Before
    public void setUp() throws Exception {
        System.out.println("开始建立与HDFS的连接");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    // 测试之后要执行的代码
    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("关闭与HDFS的连接");
    }
}