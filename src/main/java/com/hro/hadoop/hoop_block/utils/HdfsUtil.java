package com.hro.hadoop.hoop_block.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDFS 工具类
 */
public class HdfsUtil {

    private static Logger logger = LoggerFactory.getLogger(HdfsUtil.class);

    /**
     * 创建指定文件夹
     * @param parentDir
     */
    public static void createFolder(String parentDir) {
        // 定义一个配置对象
        Configuration hconf = new Configuration();
        try {
            FileSystem hfs = FileSystem.get(hconf);
            Path dirPath = new Path(parentDir);
            hfs.mkdirs(dirPath);
        } catch (Exception e) {
            logger.error("create dir error...", e);
        }
    }

    /**
     * 递归显示文件
     * @param path
     */
    public static void listFile(Path path) {
        Configuration hconf = new Configuration();
        try {
            FileSystem hfs = FileSystem.get(hconf);
            FileStatus[] statusArray = hfs.listStatus(path);
            int arrLen = statusArray.length;
            for(int i = 0; i< arrLen; i++) {
                FileStatus status = statusArray[i];
                if(status.isDirectory()) {
                    logger.info("the path is...", status.getPath());
                    HdfsUtil.listFile(status.getPath());
                }else {
                    logger.info("the path is...", status.getPath());
                }
            }
        } catch (Exception e) {
            logger.error("list file error...", e);
        }
    }

    /**
     * 上传文件
     * @param srcPath
     * @param destPath
     */
    public static void uploadFile(String srcPath, String destPath) {
        Configuration hconf = new Configuration();
        try {
            FileSystem hfs = FileSystem.get(hconf);
            Path src = new Path(srcPath);
            Path dest = new Path(destPath);
            // 从本地上传文件到服务器上
            hfs.copyFromLocalFile(src, dest);
        } catch (Exception e) {
            logger.error("upload file error...", e);
        }
    }

    /**
     * 下载文件
     * @param srcPath
     * @param destPath
     */
    public static void downloadFile(String srcPath, String destPath) {
        Configuration hconf = new Configuration();
        try {
            FileSystem hfs = FileSystem.get(hconf);
            Path src = new Path(srcPath);
            Path dest = new Path(destPath);
            // 从服务器下载文件到本地
            hfs.copyToLocalFile(src, dest);
        } catch (Exception e) {
            logger.error("download file error...", e);
        }
    }
}
