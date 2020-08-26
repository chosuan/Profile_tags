package cn.itcast.tags.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 从HBase表中读写数据，将数据封装到DataFrame中，工具类
 */
object HBaseTools {

    /**
     * 依据指定表名称、列簇及列名称，从HBase表中读取数据
     *
     * @param spark  SparkSession 实例对象
     * @param zks    Zookerper 集群地址
     * @param port   Zookeeper端口号
     * @param table  HBase表的名称
     * @param family 列簇名称
     * @param fields 列名称
     * @return
     */
    def read(spark: SparkSession, zks: String, port: String,
             table: String, family: String, fields: Seq[String]): DataFrame = {
        /*
              def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
                conf: Configuration = hadoopConfiguration,
                fClass: Class[F],
                kClass: Class[K],
                vClass: Class[V]
               ): RDD[(K, V)]
             */
        // TODO: 1. 创建Configuration对象，进行相关设置
        val conf: Configuration = HBaseConfiguration.create()
        // 1.a 设置hbase依赖zookeeper
        conf.set("hbase.zookeeper.quorum", zks)
        conf.set("hbase.zookeeper.property.clientPort", port)
        // 1.b 设置读取表的名称
        conf.set(TableInputFormat.INPUT_TABLE, table)
        // todo ： 需要设置加载列簇和列的字段过滤条件
        val scan = new Scan()
        // 设置列簇
        val familyBytes: Array[Byte] = Bytes.toBytes(family)
        scan.addFamily(familyBytes)
        // 设置列
        fields.foreach(field =>
            scan.addColumn(familyBytes, Bytes.toBytes(field))
        )
        //设置过滤
        conf.set(
            TableInputFormat.SCAN,
            Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) // 将scan对象转换为String类型
        )
        //todo  2 : 调用底层InputFormat加载HBase表的数据
        //def newAPIHadoopRDD[K, V, F <: InputFormat[K, V]](conf: Configuration = hadoopConfiguration,fClass: Class[F],kClass: Class[K],vClass: Class[V]): RDD[(K, V)]
        val datasRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        // =========================================================
        //      将RDD转换为DataFrame，采用自定义Schema方式
        // =========================================================

        // todo 3 转换RDD为DataFrame
        // 3.1 提取字段的值，封装数据结构RDD[Row]
        val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
            // a 依据字段名称获取对应值
            val values: Seq[String] = fields.map { field =>
                Bytes.toString(
                    result.getValue(familyBytes, Bytes.toBytes(field))
                )
            }
            // b 构建Row对象
            Row.fromSeq(values)
        }

        // 3.2 自定义schema信息
        val rowsSchema: StructType = StructType(
            fields.map { field => StructField(field, StringType, true) }
        )

        // 3.3. 调用createDataFrame将RDD转换为DataFrame
        spark.createDataFrame(rowsRDD, rowsSchema)
    }

    /**
     * 将DataFrame数据保存到HBase表中
     *
     * @param dataframe    数据集DataFrame
     * @param zks          Zk地址
     * @param port         端口号
     * @param table        表的名称
     * @param family       列簇名称
     * @param rowKeyColumn RowKey字段名称
     */
    def write(dataframe: DataFrame, zks: String, port: String,
              table: String, family: String, rowKeyColumn: String): Unit = {

        /*
			def saveAsNewAPIHadoopFile(
			  path: String,
			  keyClass: Class[_],
			  valueClass: Class[_],
			  outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
			  conf: Configuration = self.context.hadoopConfiguration
			): Unit
		 */
        // TODO: 1. 转换RDD[(ImmutableBytesWritable,Put)]
//        dataframe.printSchema()   //打印dataframe的Schema
        /*
        root
         |-- id: string (nullable = true)
         |-- gender: string (nullable = true)
         */
        val columns: Array[String] = dataframe.columns   //获取DataFrame种所有列的名称（“id”，“gender”）
        val cfBytes: Array[Byte] = Bytes.toBytes(family)
        val putsRDD: RDD[(ImmutableBytesWritable, Put)] = dataframe.rdd.map { row =>
            // a 获取RowKey的值
            val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](rowKeyColumn))
            // b 构建Put对象
            val put = new Put(rowKey)
            // 将每列数据加入到Put中
            columns.foreach { column =>
                put.addColumn(cfBytes, Bytes.toBytes(column), Bytes.toBytes(row.getAs[String](column)))
            }
            //c 返回二元组
            new ImmutableBytesWritable(rowKey) -> put
        }

        // TODO: 2. 构建Configuration对象
        val conf: Configuration = HBaseConfiguration.create()
        // 2.1 设置HBase依赖Zookeeper地址信息
        conf.set("hbase.zookeeper.quorum", zks)
        conf.set("hbase.zookeeper.property.clientPort", port)
        // 2.2 设置表的名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, table)

        //todo 3 调用底层API 写入HBase表数据
        putsRDD.saveAsNewAPIHadoopFile(
            s"datas/hbase/output-${System.nanoTime()}", //
            classOf[ImmutableBytesWritable],
            classOf[Put],
            classOf[TableOutputFormat[ImmutableBytesWritable]],
            conf
        )


    }

}
