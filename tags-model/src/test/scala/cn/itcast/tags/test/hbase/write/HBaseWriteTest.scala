package cn.itcast.tags.test.hbase.write

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 将画像标签数据保存至HBase表：htb_tags
 */
object HBaseWriteTest {

    def main(args: Array[String]): Unit = {

        // a. 构建SparkContext实例对象
        val sparkConf = new SparkConf()
            .setAppName("SparkHBaseWrite")
            .setMaster("local[4]")
            // 设置使用Kryo序列
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
            .registerKryoClasses(
                Array(classOf[ImmutableBytesWritable], classOf[Put])
            )
        val sc: SparkContext = new SparkContext(sparkConf)

        // b. 模拟数据集
        val tagsRDD: RDD[(String, String)] = sc.parallelize(
            List(("1001", "301,321,344,352"), ("1002", "301,323,343,352"), ("1003", "302,321,344,351")),
            numSlices = 2 //
        )

        // TODO: 将标签数据RDD保存至HBase数据库表中，需要将RDD转换为RDD[(ImmutableBytesWritable, Put)]
        /*
          HBase表：htb_tags
            RowKey：userId
            CF：user
            Column：tagIds
          create 'htb_tags', 'user'
         */
        // TODO: 第一步、转换RDD
        val cfBytes = Bytes.toBytes("user")
        val putsRDD: RDD[(ImmutableBytesWritable, Put)] = tagsRDD.map { case (userId, tagIds) =>
            // 获取RowKey
            val rowKey: Array[Byte] = Bytes.toBytes(userId)
            // 创建Put对象
            val put = new Put(rowKey)
            // 添加列
		        put.addColumn(cfBytes,Bytes.toBytes("userId"),Bytes.toBytes(userId))
		        put.addColumn(cfBytes,Bytes.toBytes("tagIds"),Bytes.toBytes(tagIds))

            // 返回二元组
            (new ImmutableBytesWritable(rowKey), put)
        }

        // TODO: 第二步、保存数据
        /*
        def saveAsNewAPIHadoopFile(
            path: String,
            keyClass: Class[_],
            valueClass: Class[_],
            outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
            conf: Configuration = self.context.hadoopConfiguration
          ): Unit
         */
        // a. 构建Configuration对象
        val conf: Configuration = HBaseConfiguration.create()
        // a.1 设置HBase依赖Zookeeper地址信息
        conf.set("hbase.zookeeper.quorum", "bigdata-cdh01.itcast.cn")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("zookeeper.znode.parent", "/hbase")
        // a.2 设置表的名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_tags")

        // b. 调用底层API，将数据保存至HBase表中
        putsRDD.saveAsNewAPIHadoopFile(
            s"datas/hbase/output-${System.nanoTime()}", //
            classOf[ImmutableBytesWritable], //
            classOf[Put], //
            classOf[TableOutputFormat[ImmutableBytesWritable]], //
            conf //
        )

        // 应用结束，关闭资源
        sc.stop()
    }

}
