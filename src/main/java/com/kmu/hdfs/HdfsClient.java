package com.kmu.hdfs;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Test;

public class HdfsClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		//1、获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.12.131:9000"), configuration, "kmu");
		//2、创建目录
		fs.mkdirs(new Path("/wuzhishan/futoubang"));
		//3、关闭资源
		fs.close();
	}


//HDFS文件上传
@Test
public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		configuration.set("dfs.replication", "2");
//		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.12.131:9000"), configuration, "kmu");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");

		// 2 上传文件
		fs.copyFromLocalFile(new Path("E:/input1/futoubang.txt"), new Path("/futoubang.txt"));

		// 3 关闭资源
		fs.close();

		System.out.println("over");
	}

//HDFS文件下载
@Test
public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
	//1、获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration,"kmu");
	
	//2、执行下载操作
	//boolean delSrc 指是否将源文件删除
	//Path src 指要下载的文件路径
	//Path dst 指将文件下载到的路径
	//boolean useRawLocalFileSystem 是否开启文件校验
	fs.copyToLocalFile(false, new Path("/banzhang.txt"),new Path("E:/output/banhua.txt"),true);
	//3、关闭资源
	fs.close();
}

//HDFS文件夹删除
@Test
public void testDelete() throws IOException, InterruptedException, URISyntaxException{

	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
		
	// 2 执行删除
	fs.delete(new Path("/0508/"), true);
		
	// 3 关闭资源
	fs.close();
}

//HDFS文件名更改
@Test
public void testRename() throws IllegalArgumentException, IOException, InterruptedException, URISyntaxException {
	//1、获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
	
	//2、修改文件名称
	fs.rename(new Path("/futoubang.txt"), new Path("/banhua.txt"));
	//3、关闭资源
	fs.close();
	}

//查看文件详情
@Test
public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
	//1、获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
	
	//2、获取文件详情
	RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/input"), true);
	while(listFiles.hasNext()) {
		LocatedFileStatus status = listFiles.next();
		//输出详情
		//文件名称
		System.out.println(status.getPath().getName());
		//长度
		System.out.println(status.getLen());
		//权限
		System.out.println(status.getPermission());
		//分组
		System.out.println(status.getGroup());
		//获取存储的块信息
		BlockLocation[] blockLocations = status.getBlockLocations();
		for (BlockLocation blockLocation : blockLocations) {
			//获取块存储的主机节点
			String[] hosts = blockLocation.getHosts();
			for (String host : hosts) {
				System.out.println(host);
			}
		}
		
		System.out.println("----------分割线----------------------");
		
	}
	//3、关闭资源
	fs.close();
}

//HDFS文件和文件夹判断
@Test
public void testListStatus() throws IOException, InterruptedException, URISyntaxException{
		
	// 1 获取文件配置信息
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
		
	// 2 判断是文件还是文件夹
	FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
	for (FileStatus fileStatus : listStatus) {
		
		// 如果是文件
		if (fileStatus.isFile()) {
				System.out.println("f:"+fileStatus.getPath().getName());
			}else {
				System.out.println("d:"+fileStatus.getPath().getName());
			}
		}
		
	// 3 关闭资源
	fs.close();
}

//HDFS的I/O流操作
//HDFS文件上传
@Test
public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
	//1、获取文件系统
	Configuration configurtion = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configurtion, "kmu");
	//2、创建输入流
	FileInputStream fis = new FileInputStream(new File("E:/input1/banhua.txt"));
	//3、获取输出流
	FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));
	//4、流对拷
	IOUtils.copyBytes(fis, fos, configurtion);
	
	//5、关闭资源
	IOUtils.closeStream(fos);
	IOUtils.closeStream(fis);
	fs.close();
	
	}

//HDFS文件下载
@Test
public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
	//1、获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
	//2、获取输入流
	FSDataInputStream fis = fs.open(new Path("/aa.txt"));
	//3、获取输出流
	FileOutputStream fos = new FileOutputStream(new File("E:/output/aa.txt"));
	//4、流的对拷
	IOUtils.copyBytes(fis, fos, configuration);
	
	//5、关闭资源
	IOUtils.closeStream(fos);
	IOUtils.closeStream(fis);
	fs.close();
	}
@Test
public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
		
	// 2 获取输入流
	FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
		
	// 3 创建输出流
	FileOutputStream fos = new FileOutputStream(new File("E:/output/banhua.txt"));
		
	// 4 流的拷贝
	byte[] buf = new byte[1024];
		
	for(int i =0 ; i < 1024 * 128; i++){
		fis.read(buf);
		fos.write(buf);
	}
		
	// 5关闭资源
	IOUtils.closeStream(fis);
	IOUtils.closeStream(fos);
	fs.close();
	}

@Test
public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

	// 1 获取文件系统
	Configuration configuration = new Configuration();
	FileSystem fs = FileSystem.get(new URI("hdfs://hadoop131:9000"), configuration, "kmu");
		
	// 2 打开输入流
	FSDataInputStream fis = fs.open(new Path("/banhua.txt"));
		
	// 3 定位输入数据位置
	fis.seek(1024*1024*128);
		
	// 4 创建输出流
	FileOutputStream fos = new FileOutputStream(new File("E:/output/banhua.txt"));
		
	// 5 流的对拷
	IOUtils.copyBytes(fis, fos, configuration);
		
	// 6 关闭资源
	IOUtils.closeStream(fis);
	IOUtils.closeStream(fos);
	fs.close();
}


}
