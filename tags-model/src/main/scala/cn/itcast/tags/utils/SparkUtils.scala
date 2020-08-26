package cn.itcast.tags.utils

import java.util
import java.util.Map

import cn.itcast.tags.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 创建SparkSession对象工具类
 */
object SparkUtils {
	
	
	/**
	 * 加载Spark Application默认配置文件，设置到SparkConf中
	 * @param resource 资源配置文件名称
	 * @return SparkConf对象
	 */
	def loadConf(resource: String): SparkConf = {
		// 1. 创建SparkConf 对象
		val sparkConf = new SparkConf()
		// 2. 使用ConfigFactory加载配置文件
		val config: Config = ConfigFactory.load(resource)
		// 3. 获取加载配置信息
		val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
		// 4. 循环遍历设置属性值到SparkConf中
		import scala.collection.JavaConverters._
		entrySet.asScala.foreach{entry =>
			// 获取属性来源的文件名称
			val resourceName = entry.getValue.origin().resource()
			if(resource.equals(resourceName)){
				sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
			}
		}
		// 5. 返回SparkConf对象
		sparkConf
	}
	
	/**
	 * 构建SparkSession实例对象，如果是本地模式，设置master
	 */
	def createSparkSession(clazz: Class[_], isHive: Boolean = false): SparkSession = {
		// 1. 获取SparkConf对象
		val sparkConf = loadConf("spark.properties")
		// 2. 判断是否是本地模式
		if(ModelConfig.APP_IS_LOCAL){
			sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
		}
		// 3. 创建SparkSession.Builder对象
		val builder: SparkSession.Builder = SparkSession.builder()
    		.config(sparkConf)
    		.appName(clazz.getSimpleName.stripSuffix("$"))
		// 4. 判断是否集成Hive
		if(ModelConfig.APP_IS_HIVE || isHive){
			builder
    			.enableHiveSupport()
    			.config("hive.metastore.uris", ModelConfig.APP_HIVE_META_STORE_URL)
		}
		// 5. 创建对象
		builder.getOrCreate()
	}
	
	
}
