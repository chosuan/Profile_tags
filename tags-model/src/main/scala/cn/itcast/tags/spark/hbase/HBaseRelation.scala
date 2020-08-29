package cn.itcast.tags.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class HBaseRelation(context: SQLContext, //
                    params: Map[String, String], //
                    userSchema: StructType //
                    ) extends BaseRelation with TableScan with InsertableRelation with Serializable {

    // 连接HBase数据库的属性名称
    val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
    val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
    val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
    val HBASE_ZK_PORT_VALUE: String = "zkPort"
    val HBASE_TABLE: String = "hbaseTable"
    val HBASE_TABLE_FAMILY: String = "family"
    val SPERATOR: String = ","
    val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
    val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

    // filterConditions：modified[GE]20190601,modified[LE]20191201
    val HBASE_TABLE_FILTER_CONDITIONS: String = "filterConditions"

    /**
     * Spark 1.x中读取数据入口SQLContext，相当于Spark2.x以后SparkSession，属于历史遗留
     */
    override def sqlContext: SQLContext = context

    /**
     * 数据Schema信息，封装在StructType中
     */
    override def schema: StructType = userSchema

    /**
     * 从外部数据源加载数据，比如从HBase表读取数据，封装到RDD[Row]中
     */
    override def buildScan(): RDD[Row] = {

        // 获取查询字段Fields
        val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)

        // TODO: 1. 创建Configuration对象，进行相关设置
        val conf: Configuration = HBaseConfiguration.create()
        // 1.a 设置hbase依赖zookeeper
        conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
        conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
        // 1.b 设置读取表的名称
        conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))
        // todo ： 需要设置加载列簇和列的字段过滤条件
        val scan = new Scan()
        // 设置列簇
        val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
        scan.addFamily(familyBytes)
        // 设置列
        fields.foreach(field =>
            scan.addColumn(familyBytes, Bytes.toBytes(field))
        )



        // TODO: 添加要设置过滤器Filter，比如依据某个字段的值进行过滤操作，
        // ===================================================================
        // step 1：获取过滤条件值
        val filterConditions: String = params.getOrElse(HBASE_TABLE_FILTER_CONDITIONS, null)

        // step 2: 判断过滤条件是否有值 ，有值进行设置过滤器filter
        if(null != filterConditions && filterConditions.trim.split(",").length > 0){

            val filterList = new FilterList()
            filterConditions
                .split(SPERATOR)  // modified[GE]20190601  、 modified[LE]20191201
                .foreach{ filterCondition =>
                    // i. 解析过滤条件，封装Condition对象中
                    val condition: Condition = Condition.parseCondition(filterCondition)
                    val filter = new SingleColumnValueFilter(    ////   modified[GE]20190601
                        familyBytes,    //列簇
                        Bytes.toBytes(condition.field),   //列
                        condition.compare,    //比较器
                        Bytes.toBytes(condition.value)   //   字段值
                    )
                    // iii. 将过滤器Filter加入至List中
                    filterList.addFilter(filter)
                    // iv. TODO: 当使用过滤器时，确定过滤字段一定要读取值，否则无法过滤
                    scan.addColumn(familyBytes , Bytes.toBytes(condition.field))
                }
            // step 3: 设置过滤器
            scan.setFilter(filterList)
        }
        // ===================================================================
        // ===================================================================



        //设置过滤
        conf.set(
            TableInputFormat.SCAN,
            Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray) // 将scan对象转换为String类型
        )
        //todo  2 : 调用底层InputFormat加载HBase表的数据
        //def newAPIHadoopRDD[K, V, F <: InputFormat[K, V]](conf: Configuration = hadoopConfiguration,fClass: Class[F],kClass: Class[K],vClass: Class[V]): RDD[(K, V)]
        val datasRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
            conf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
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
        rowsRDD
    }

    /**
     * 将数据集DataFrame保存至外部数据源，比如HBase表中
     * @param data 分布式数据集
     * @param overwrite 保存数据时，是否覆写
     */
    override def insert(data: DataFrame, overwrite: Boolean): Unit = {

        // TODO: 1. 转换RDD[(ImmutableBytesWritable,Put)]
        //        dataframe.printSchema()   //打印dataframe的Schema
        val columns: Array[String] = data.columns   //获取DataFrame种所有列的名称（“id”，“gender”）
        val cfBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
        val putsRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map { row =>
            // a 获取RowKey的值
            val rowKey: Array[Byte] = Bytes.toBytes(row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME)))
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
        conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
        conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))
        // 1.b 设置读取表的名称
        conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))

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
