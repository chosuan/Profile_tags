package cn.itcast.tags.models

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.{HBaseTools, ProfileTools}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 标签基类，各个标签模型继承此类，实现其中打标签方法doTag即可
 */
trait BasicModel extends Logging {

    // 变量声明
    var spark: SparkSession = _

    // 1. 初始化：构建SparkSession实例对象
    def init(): Unit = {
        // a. 创建SparkConf 设置应用信息
        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
            .setMaster("local[4]")
            .set("spark.sql.shuffle.partitions", "4")
            // 由于从HBase表读写数据，设置序列化
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(
                Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put])
            )
        // b. 建造者模式构建SparkSession对象
        spark = SparkSession.builder()
            .config(sparkConf)
            // TODO：与Hive集成，读取Hive表的数据
            //.enableHiveSupport()
            //.config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
            //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
            .getOrCreate()
    }

    // 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
    def getTagData(tagId: Long): DataFrame = {
        // 依据标签ID：tagId(4级标签、业务标签），查询标签相关数据
        val tagTable: String =
            s"""
			  |(
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE id = $tagId
			  |UNION
			  |SELECT `id`,
			  |       `name`,
			  |       `rule`,
			  |       `level`
			  |FROM `profile_tags`.`tbl_basic_tag`
			  |WHERE pid =  $tagId
			  |ORDER BY `level` ASC, `id` ASC
			  |) AS basic_tag
			  |""".stripMargin
        // 使用SparkSQL内置JDBC方式读取MySQL表的数据
        spark.read
            .format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url",
                "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
            .option("dbtable", tagTable)
            .option("user", "root")
            .option("password", "123456")
            .load()
    }

    // 3. 业务数据：依据业务标签规则rule，从数据源获取业务数据
    def getBusinessData(tagDF: DataFrame): DataFrame = {
        import tagDF.sparkSession.implicits._

        // a. 获取业务标签数据的标签规则rule
        val tagRule: String = tagDF
            .filter($"level" === 4) // 4 表示业务标签
            .head() // 获取第一条数据
            // 获取规则
            .getAs[String]("rule")
        // b. 解析规则，存储至Map集合
        val ruleMap: Map[String, String] = tagRule
            // 先按照换行符分割
            .split("\\n")
            // 再按照等号分割
            .map { line =>
                val Array(attrKey, attrValue) = line.trim.split("=")
                attrKey -> attrValue
            }
            // 由于数组中数据类型为二元组，可以直接转换为Map集合
            .toMap
        logWarning(s"============ { ${ruleMap.mkString(", ")} } ===========")
        // c. 判断数据源，依据数据源获取业务数据
        var businessDF: DataFrame = null
        if ("hbase".equals(ruleMap("inType").toLowerCase)) {
            // 将Map集合中数据源信息封装到Meta表中
            val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
            // 从HBase表加载业务数据
            businessDF = HBaseTools.read(
                spark, hbaseMeta.zkHosts, hbaseMeta.zkPort, //
                hbaseMeta.hbaseTable, hbaseMeta.family, //
                hbaseMeta.selectFieldNames.split(",").toSeq //
            )
        } else {
            // 如果未获取到数据，直接抛出异常
            new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
        }
        // 返回业务数据
        businessDF
    }

    // 4. 构建标签：依据业务数据和属性标签数据建立标签
    def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame

    // 5. 标签合并与保存：读取用户标签数据，进行合并操作，最后保存
    def mergeAndSaveTag(modelDF: DataFrame): Unit = {
        // a. 获取HBase数据库中历史画像标签表的数据:profileDF -> tbl_profile
        val profileDF: DataFrame = ProfileTools.loadProfile(spark)
        // b. 合并当前标签数据和历史画像标签数据：newProfileDF
        val newProfileDF: DataFrame = ProfileTools.mergeProfileTags(modelDF, profileDF)
        // c. 保存最新的画像标签数据至HBase数据库
        ProfileTools.saveProfile(newProfileDF)
    }

    // 6. 关闭资源：应用结束，关闭会话实例对象
    def close(): Unit = {
        if (null != spark) spark.close()
    }

    // 模板方法：确定标签模型执行顺序
    def executeModel(tagId: Long): Unit = {
        // a. 初始化
        init()
        try {
            // b. 获取标签数据
            val tagDF: DataFrame = getTagData(tagId)
            //basicTagDF.show()
            tagDF.persist(StorageLevel.MEMORY_ONLY_2).count()

            // c. 获取业务数据
            val businessDF: DataFrame = getBusinessData(tagDF)
            //businessDF.show()

            // d. 计算标签
            val modelDF: DataFrame = doTag(businessDF, tagDF)
            //modelDF.show()

            // e. 合并标签与保存
            mergeAndSaveTag(modelDF)

            tagDF.unpersist()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            close()
        }
    }

}
