package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object JobModel extends Logging{

    /*
      321	职业
        322	学生		1
        323	公务员	2
        324	军人		3
        325	警察		4
        326	教师		5
        327	白领		6
	 */
    def main(args: Array[String]): Unit = {

//        System.load("D:\\Program Files\\hadoop-2.6.0\\bin\\hadoop.dll")

        // 构建SparkSession实例对象，用于加载数据源数据
        val spark: SparkSession = {
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
            val session = SparkSession.builder()
                .config(sparkConf)
//                // TODO：与Hive集成，读取Hive表的数据
//                .enableHiveSupport()
//                .config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
//                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .getOrCreate()
            // c. 返回会话实例对象
            session
        }
        import spark.implicits._  // 导入隐式转换

        // TODO: 2. 从MySQL数据库读取标签数据（基础标签表：tbl_basic_tag），依据业务标签ID读取
        /**
         * 318 性别 4
         * 319 男=1 5
         * 320 女=2 5
         */
        val tagTable: String =
            """
              |(
              |SELECT `id`,
              | `name`,
              | `rule`,
              | `level`
              |FROM `profile_tags`.`tbl_basic_tag`
              |WHERE id = 317
              |UNION
              |SELECT `id`,
              | `name`,
              | `rule`,
              | `level`
              |FROM `profile_tags`.`tbl_basic_tag`
              |WHERE pid = 317
              |ORDER BY `level` ASC, `id` ASC
              |) AS basic_tag
              |""".stripMargin
        val basicTagDF: DataFrame = spark.read
            .format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
            .option("dbtable", tagTable)
            .option("user", "root")
            .option("password", "123456")
            .load()

        basicTagDF.printSchema()
        /*
        root
         |-- id: long (nullable = false)
         |-- name: string (nullable = true)
         |-- rule: string (nullable = true)
         |-- level: integer (nullable = true)
         */
        basicTagDF.show(20,false)
        /*
        +---+----+-----------------------------------------------------------------------------------------------------------------------+-----+
        |id |name|rule                                                                                                                   |level|
        +---+----+-----------------------------------------------------------------------------------------------------------------------+-----+
        |321|职业  |inType=hbase
        zkHosts=bigdata-cdh01.itcast.cn
        zkPort=2181
        hbaseTable=tbl_tag_users
        family=detail
        selectFieldNames=id,job|4    |
        |322|学生  |1                                                                                                                      |5    |
        |323|公务员|2                                                                                                                      |5    |
        |324|军人  |3                                                                                                                      |5    |
        |325|警察  |4                                                                                                                      |5    |
        |326|教师  |5                                                                                                                      |5    |
        |327|白领  |6                                                                                                                      |5    |
        +---+----+-----------------------------------------------------------------------------------------------------------------------+-----+
         */
        // 由于标签数据被使用2次，从MySQL数据库读取，建议缓存
        basicTagDF.persist(StorageLevel.MEMORY_ONLY_2).count()

        // TODO: 3. 依据业务标签规则获取业务数据，比如到HBase数据库读取表的数据
        // 3.1. 4级标签规则rule
        val tagRule: String = basicTagDF.filter($"level" === 4) //4 表示业务标签
            .head() //获取第一条数据
            .getAs[String]("rule") //获取规则
        logInfo(s"===================[ ${tagRule} ]====================")
        /*
        ===================[ inType=hbase
            zkHosts=bigdata-cdh01.itcast.cn
            zkPort=2181
            hbaseTable=tbl_tag_users
            family=detail
            selectFieldNames=id,job ]====================
         */

        // 3.2. 解析标签规则，先按照换行\n符分割，再按照等号=分割
        /*
        inType=hbase
        zkHosts=bigdata-cdh01.itcast.cn
        zkPort=2181
        hbaseTable=tbl_tag_users
        family=detail
        selectFieldNames=id,job
        */
        val ruleMap: Map[String, String] = tagRule
            //先按照换行符\\n分割
            .split("\\n")
            //再按照等号分割
            .map { line =>
                val Array(attrKey, attrValue) = line.trim.split("=")
                attrKey -> attrValue
            }
            //由于数组中数据类型为二元组，可以直接转换为Map集合
            .toMap
        logWarning(s"============ { ${ruleMap.mkString(", ")} } ===========")

        // 3.3. 判断数据源，依据标签规则中inType类型获取数据
        var businessDF: DataFrame = null
        if ("hbase".equals(ruleMap("inType").toLowerCase)) {
            //将Map集合中数据源信息封装到Meta表中
            val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
            //从HBase表中加载业务数据
            businessDF = HBaseTools.read(
                spark, hbaseMeta.zkHosts, hbaseMeta.zkPort,
                hbaseMeta.hbaseTable, hbaseMeta.family, hbaseMeta.selectFieldNames.split(",").toSeq
            )
        } else {
            //如果未获取到数据，直接抛出异常
            new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
        }
        businessDF.printSchema()
        /*
        root
         |-- id: string (nullable = true)
         |-- job: string (nullable = true)
         */
        businessDF.show(20, false)
        /*
        +---+---+
        |id |job|
        +---+---+
        |1  |3  |
        |10 |5  |
        |100|3  |
        |101|1  |
        |102|1  |
         */

        // TODO: 4. 业务数据和属性标签数据结合，构建标签：规则匹配型标签 -> rule match
        // 4.1 首先获取属性标签数据：id->tagId, rule， 转换为Map集合
        val attrTagMap: Map[String, Long] = basicTagDF
            .filter($"level" === 5)
            .select(
                $"rule",
                $"id".as("tagId")
            )
            .as[(String, Long)]
            .rdd
            .collectAsMap().toMap
        logInfo(s"=================================${attrTagMap}===================================")
        /*
        =================================
                    Map(4 -> 325,  2 -> 323, 5 -> 326, 1 -> 322, 6 -> 327, 3 -> 324)
        ===================================
         */

        //广播变量
        val broadcastMap: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(attrTagMap)

        // 4.2 自定义UDF函数，传入job，获取tagId
        val job_to_tag: UserDefinedFunction = udf(
            (job: String) => { broadcastMap.value(job)}
        )
        // 3.3 使用udf函数，给每个用户打上标签的值：tagId
        val modelDF: DataFrame = businessDF.select(
            $"id".as("uid"), //
            job_to_tag($"job").cast(StringType).as("tagId")
        )
        modelDF.persist(StorageLevel.MEMORY_ONLY_2)
        modelDF.printSchema()
        /*
        root
         |-- uid: string (nullable = true)
         |-- tagId: string (nullable = true)
         */
        modelDF.show(100,false)

        // 当缓存数据不再被使用时，释放资源
        basicTagDF.unpersist()
//        modelDF.unpersist()


        // TODO: 5. 将标签数据存储到HBase表中：用户画像标签表 -> tbl_profile
        // 5.1. 从HBase表中读取户画像标签表数据: rowkey(userId)、tagIds      profileDF -> tbl_profile
        val profileDF: DataFrame = HBaseTools.read(
            spark, "bigdata-cdh01.itcast.cn", "2181", //
            "tbl_profile", "user", Seq("userId", "tagIds")
        )

        // 5.2. 合并当前标签数据和历史画像标签数据：newProfileDF
        // a. 按照用户ID关联数据，使用左外连接
        val mergeDF: DataFrame = modelDF.join(
            profileDF, modelDF("uid") === profileDF("userId"), "left"
        )
        mergeDF.printSchema()
        /*
        root
         |-- uid: string (nullable = true)
         |-- tagId: string (nullable = false)
         |-- userId: string (nullable = true)
         |-- tagIds: string (nullable = true)
         */
        mergeDF.show(20, false)
        /*
        +---+-----+------+-----------+
        |uid|tagId|userId|tagIds     |
        +---+-----+------+-----------+
        |1  |320  |1     |320        |
        |102|320  |102   |209,200,320|
        |107|319  |107   |319        |
        |110|320  |110   |320        |
         */

        // b. 自定义UDF函数，合并标签
        val merge_tags_udf: UserDefinedFunction = udf {
            (tagId: String, tagIds: String) => {
                tagIds
                    .split(",")
                    .:+(tagId)
                    .distinct
                    .mkString(",")
            }
        }

        // c. 获取最新用户画像标签数据
        val newProfileDF: DataFrame = mergeDF.select(
            $"uid".as("userId"),
            // 5.3. 当tagIds不为null时，合并已有标签与计算标签
            when($"tagIdS".isNull, $"tagId")
                .otherwise(merge_tags_udf($"tagId", $"tagIds"))
                .as("tagIds")
        )
        newProfileDF.printSchema()
        /*
        root
         |-- userId: string (nullable = true)
         |-- tagIds: string (nullable = true)
         */
        newProfileDF.show(10, truncate = false)
        /*
        +------+-----------+
        |userId|tagIds     |
        +------+-----------+
        |1     |320        |
        |102   |209,200,320|
        |107   |319        |
        |110   |320        |
         */

        // 5.4. 保存HBase表中
        HBaseTools.write(
            newProfileDF, "bigdata-cdh01.itcast.cn", "2181", //
            "tbl_profile", "user", "userId"
        )


        spark.stop()

    }

}
