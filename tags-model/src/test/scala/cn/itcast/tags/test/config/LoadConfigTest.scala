package cn.itcast.tags.test.config

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf

object LoadConfigTest {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()

        //加载属性配置文件
        val config: Config = ConfigFactory.load("spark.conf")
        //获取所有集合
        val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()
        //遍历
        import scala.collection.JavaConverters._

        entrySet.asScala.foreach { entry =>
            val resource: String = entry.getValue.origin().resource()
            if ("spark.conf".equals(resource)) {
                println(s"resource = ${entry.getValue.origin().resource()}")
                println(s"key = ${entry.getKey}  value = ${entry.getValue.unwrapped().toString} ")
                sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
            }
        }
    }

}
