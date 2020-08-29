package cn.itcast.tags.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * 操作HDFS文件系统工具类
 */
object HdfsUtils {
	
	/**
	 * 判断路径是否存在
	 * @param conf Configuration 实例对象
	 * @param path 模型路径
	 * @return
	 */
	def exists(conf: Configuration, path: String): Boolean = {
		// a. 获取文件系统
		val dfs: FileSystem = FileSystem.get(conf)
		// b. 判断路径是否存在
		dfs.exists(new Path(path))
	}
	
	/**
	 * 删除路径Path
	 * @param conf Configuration 实例对象
	 * @param path 模型路径
	 */
	def delete(conf: Configuration, path: String): Unit ={
		// a. 获取文件系统
		val dfs: FileSystem = FileSystem.get(conf)
		// b. 如果路径存在就删除
		dfs.deleteOnExit(new Path(path))
	}
	
	
	def main(args: Array[String]): Unit = {
		val conf = new Configuration()
		conf.set("fs.defaultFS", "hdfs://bigdata-cdh01.itcast.cn:8020")
		
		// hdfs dfs -mkdir -p /apps/models/rfmmodel
		val isExists: Boolean = exists(conf, "/apps/models/rfmmodel")
		println(s"isExists = $isExists")
		
		delete(conf, "/apps/models/rfmmodel")
	}

}