package cn.itcast.tags.spark.hbase

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


class DefaultSource extends RelationProvider with CreatableRelationProvider
    with DataSourceRegister with Serializable{

    // 参数信息
    val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
    val SPERATOR: String = ","

    /**
     * 方便使用数据源时，采用简写方式，spark.read.format("hbase")
     */
    override def shortName(): String = "hbase"

    /**
     * 提供加载外部数据源Relation对象，此处就是HBaseRelation对象，自动创建
     * @param sqlContext 程序入口，Spark1.x中SQLContext对象
     * @param parameters 加载外部数据源数据时，传递参数封装的Map集合
     */
    override def createRelation(sqlContext: SQLContext,
                                parameters: Map[String, String]): BaseRelation = {


        // 1. 定义Schema信息
        val schema: StructType = StructType(
            parameters(HBASE_TABLE_SELECT_FIELDS)
                .split(SPERATOR)
                .map{field => StructField(field, StringType, nullable = true)}
        )
        // 2. 创建HBaseRelation对象----返回给
        val relation = new HBaseRelation(sqlContext, parameters, schema)
        // 3. 返回对象
        relation

    }


    /**
     * 提供保存数据DataFrame至外部数据源Relation对象，此处就是HBaseRelation对象，自动创建
     * @param sqlContext 程序入口，Spark1.x中SQLContext对象
     * @param mode 保存数据模式
     * @param parameters 保存数据至外部数据源时，传递参数封装的Map集合
     * @param data 要保存的数据
     */
    override def createRelation(
                                   sqlContext: SQLContext,
                                   mode: SaveMode,
                                   parameters: Map[String, String],
                                   data: DataFrame): BaseRelation = {

        // 1. 获取HBaseRelation对象
        val relation = new HBaseRelation(sqlContext, parameters, data.schema)
        // 2. 插入数据
        val overwrite: Boolean = mode match {
            case SaveMode.Overwrite => true
            case _ => false
        }
        relation.insert(data, overwrite)
        // 3. 返回对象
        relation
    }


}
