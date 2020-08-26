package cn.itcast.tags.models.rule

import cn.itcast.tags.meta.HBaseMeta
import cn.itcast.tags.tools.HBaseTools
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * 用户性别标签模型
 */
object GenderModel extends Logging {

    def main(args: Array[String]): Unit = {


        // TODO: 1. 创建SparkSession实例对象
        val spark: SparkSession = {
            //  1.a. 创建SparkConf 设置应用信息
            val sparkConf = new SparkConf()
                .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
                .setMaster("local[4]")
                .set("spark.sql.shuffle.partitions", "4")
                // 由于从HBase表读写数据，设置序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(
                    Array(classOf[ImmutableBytesWritable],
                        classOf[Result],
                        classOf[Put])
                )

            // 1.b. 建造者模式构建SparkSession对象
            val session = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate()
            // 1.c. 返回会话实例对象
            session
        }
        import spark.implicits._


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
              |WHERE id = 314
              |UNION
              |SELECT `id`,
              | `name`,
              | `rule`,
              | `level`
              |FROM `profile_tags`.`tbl_basic_tag`
              |WHERE pid = 314
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
        /*
        root
        |-- id: long (nullable = false)
        |-- name: string (nullable = true)
        |-- rule: string (nullable = true)
        |-- level: integer (nullable = true)
        */
        basicTagDF.printSchema()
        basicTagDF.show(20, truncate = false)
        //由于标签数据被使用两次，从MySQL数据库读取，建议缓存
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
            selectFieldNames=id,gender ]====================
         */

        // 3.2. 解析标签规则，先按照换行\n符分割，再按照等号=分割
        /*
        inType=hbase
        zkHosts=bigdata-cdh01.itcast.cn
        zkPort=2181
        hbaseTable=tbl_tag_users
        family=detail
        selectFieldNames=id,gender
        */
        val ruleMap: Map[String, String] = tagRule
            //先按照换行符\\n分割
            .split("\\n")
            //再按照等号分割
            .map { line =>
                val Array(attrKey, attrValue) = line.trim.split("=")
                (attrKey, attrValue)
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
         |-- gender: string (nullable = true)
         */
        businessDF.show(20, false)
        /*
        +---+------+
        |id |gender|
        +---+------+
        |1  |2     |
        |10 |2     |
        |100|2     |
        |101|1     |
        |102|2     |
        |103|1     |
        |104|1     |
        |105|2     |
         */


        // TODO: 4. 业务数据和属性标签结合，构建标签：规则匹配型标签 -> rule match
        // 4.1. 获取5级标签对应TagId和TagRule,,,首先获取属性标签数据：id->tagId, rule
        val attrTagDF: DataFrame = basicTagDF
            .filter($"level" === 5)
            .select(
                $"id".as("tagId"),
                $"rule"
            )

        // 4.2. DataFrame 关联，依据属性标签规则rule与业务数据字段gender
        val joinDF: DataFrame = businessDF.join(
            attrTagDF, businessDF("gender") === attrTagDF("rule")
        )
        joinDF.printSchema()
        /*
        root
         |-- id: string (nullable = true)
         |-- gender: string (nullable = true)
         |-- tagId: long (nullable = false)
         |-- rule: string (nullable = true)
         */
        joinDF.show(20, false)
        /*
        +---+------+-----+----+
        |id |gender|tagId|rule|
        +---+------+-----+----+
        |1  |2     |320  |2   |
        |10 |2     |320  |2   |
        |100|2     |320  |2   |
        |101|1     |319  |1   |
         */
        val modelDF: DataFrame = joinDF.select(
            $"id".as("uid"), $"tagId".cast(StringType)
        )
        modelDF.printSchema()
        /*
        root
         |-- uid: string (nullable = true)
         |-- tagId: long (nullable = false)
         */
        modelDF.show(20, false)
        /*
        +---+-----+
        |uid|tagId|
        +---+-----+
        |1  |320  |
        |10 |320  |
        |100|320  |
        |101|319  |
         */

        // 当缓存数据不再被使用时，释放资源
        basicTagDF.unpersist()


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

        // 应用结束，关闭资源
        spark.stop()

    }

}
