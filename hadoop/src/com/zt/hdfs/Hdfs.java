package com.zt.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;


public class Hdfs {
	private static Configuration configuration;
	private static URI uri;
	private static FileSystem fileSystem;
	private static FileSystem localSystem;
	
	public static void main(String [] args) throws IOException, URISyntaxException{
		
		init();
//		getFileStatus("/data/a.txt");
//		deleteFileOrDir("/data/a.txt",true);
//		mkdir("/data");
//		createFile("/data/b.txt");
//		getFileStatusFromDir("/data");
//		writeFile("/data/b.txt");
//		readFile("/data/b.txt");
//		deleteFileOrDir("/test", true);
		mergeFile("/home/hadoop/test/", "/test/merge.txt");
	}
	
	
	/*
	 * initialize the Haddop Cluster configuration
	 */
	public static void init() throws URISyntaxException, IOException{
		configuration = new Configuration();
		uri = new URI("hdfs://Master:9000");
		fileSystem = (DistributedFileSystem) FileSystem.get(uri, configuration);
		localSystem = FileSystem.getLocal(configuration);
		
	}
	

	/*
	 * create File 
	 * 
	 */
	
	public static void createFile(String fileName) throws IllegalArgumentException, IOException{
		FSDataOutputStream outputStream = fileSystem.create(new Path(fileName));
		outputStream.close();
		
	}
	
	public static void readFile(String fileName) throws IllegalArgumentException, IOException{
		FSDataInputStream inputStream = fileSystem.open(new Path(fileName));
		IOUtils.copyBytes(inputStream, System.out, configuration);
		IOUtils.closeStream(inputStream);
	}
	
	public static void writeFile(String fileName) throws IllegalArgumentException, IOException{
		FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(fileName));
		byte[] bytes = "hello, hadoop".getBytes();
		fsDataOutputStream.write(bytes, 0, bytes.length);
		fsDataOutputStream.close();
	}
	
	/*
	 * create a directory
	 */
	public static void mkdir(String dirName) throws IOException{
		Path dir = new Path(dirName);
		boolean flag = fileSystem.mkdirs(dir);
		if (flag) {
			System.out.println("success to mkdir " + dirName);
		}else {
			System.out.println("fialed to mkdir " + dirName);
		}
		
	}
	
	
	
	/*
	 * delete a file or directory 
	 */
	public static void deleteFileOrDir(String dirName,boolean recursive) throws IllegalArgumentException, IOException {
		boolean flag = fileSystem.delete(new Path(dirName), recursive);
		System.out.println("flag=" + flag);
	
	}
	
	/*
	 * list a detail information about a file
	 */
	public static void getFileStatus(String filePath) throws IOException {
		Path path = new Path(filePath);
		FileStatus fileStatus = fileSystem.getFileStatus(path);
		System.out.println(fileStatus);
		System.out.println("path= " +fileStatus.getPath() );
		System.out.println("IsDirectory=" + fileStatus.isDirectory());
		System.out.println("length:" + fileStatus.getLen());
		System.out.println("replication=" + fileStatus.getReplication());
		System.out.println("owner:" + fileStatus.getOwner());
		System.out.println("group:" + fileStatus.getGroup());
		System.out.println("Permission:" + fileStatus.getPermission());
		System.out.println("isSymlink:" + fileStatus.isSymlink());
		System.out.println("blockSize=" + fileStatus.getBlockSize() / 1024 / 1024 + "M");
		
	}
	
	/*
	 * get all files detail information  in a directory
	 */
	public static void getFileStatusFromDir(String dirName) throws FileNotFoundException, IllegalArgumentException, IOException{
		FileStatus[] listStatus = fileSystem.listStatus(new Path(dirName));
		for (FileStatus fileStatus : listStatus) {
			System.out.println(fileStatus);
		}
		
	}
	
	/*
	 * merge file from local file system to Hdfs
	 */
	public static void mergeFile(String dirName,String hdfsFileName) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		FileStatus[] listStatus = localSystem.listStatus(new Path(dirName));
		Path hdfsfile = new Path(hdfsFileName);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsfile);
		for (FileStatus fileStatus : listStatus) {
			Path path = fileStatus.getPath();
			FSDataInputStream fsDataInputStream = localSystem.open(path);
			byte[] buffer = new byte[256];
			int len ;
			while((len = fsDataInputStream.read(buffer)) > 0){
				fsDataOutputStream.write(buffer, 0, len);
			}
			fsDataInputStream.close();
		}
		fsDataOutputStream.close();
		
		
	}
	
}
