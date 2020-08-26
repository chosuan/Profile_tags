package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}

class AgeRangeModel extends AbstractModel("年龄段标签", ModelType.STATISTICS) {

    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

        /*
        335	年龄段		selectFieldNames=id,birthday
            336	50后		19500101-19591231
            337	60后		19600101-19691231
            338	70后		19700101-19791231
            339	80后		19800101-19891231
            340	90后		19900101-19991231
            341	00后		20000101-20091231
            342	10后		20100101-20191231
            343	20后		20200101-20291231
         */
        //businessDF.printSchema()
        /*
        root
         |-- id: string (nullable = true)
         |-- birthday: string (nullable = true)
         */
        //businessDF.show(20,false)   //   |103|1987-05-13|

        //println("+"*40)
        //tagDF.printSchema()
        /*
        root
         |-- id: long (nullable = false)
         |-- name: string (nullable = true)
         |-- rule: string (nullable = true)
         |-- level: integer (nullable = true)
         */
        //tagDF.show(20,false)       //    |339|80后 |19800101-19891231   |5    |

        // 导入隐式转换
        import businessDF.sparkSession.implicits._

        //1 自定义UDF函数，解析分解属性标签的规则rule，   19800101-19891231
        val rule_to_udf: UserDefinedFunction = udf(
            (rule: String) => {
                val Array(start, end) = rule.split("-").map(_.toInt)
                // 返回二元组
                (start, end)
            }
        )

        // 2. 获取属性标签数据，解析规则rule
        val attrTagDF: DataFrame = tagDF
            .filter($"level" === 5)
            .select(
                $"id".as("tagId"),
                rule_to_udf($"rule").as("rules")
            )
            // 获取起始start和结束end
            .select(
                $"tagId",
                $"rules._1".as("start"), $"rules._2".as("end")
            )

//        attrTagDF.printSchema()
//        attrTagDF.show(20,false)

        // 3. 业务数据与标签规则关联JOIN，比较范围
        /*
            attrTagDF： attr
            businessDF: business
            SELECT t2.userId, t1.tagId FROM attr t1 JOIN business t2
            WHERE t1.start <= t2.birthday AND t1.end >= t2.birthday ;
        */
        // 3.1. 转换日期格式： 1982-01-11 -> 19820111
        val birthdayDF: DataFrame = businessDF
            .select(
                $"id".as("uid"),
                regexp_replace($"birthday", "-", "")
                    .cast(IntegerType).as("bornDate")
            )
        // 3.2. 关联属性规则，设置条件
        val modelDF: DataFrame = birthdayDF.join(attrTagDF)
            .where(birthdayDF("bornDate").between(attrTagDF("start"), attrTagDF("end")))
            .select($"uid", $"tagId".cast(StringType))

//        modelDF.printSchema()
//        modelDF.show(20,false)

        modelDF
    }
}

object AgeRangeModel {
    def main(args: Array[String]): Unit = {

        val tagModel = new AgeRangeModel
        tagModel.executeModel(335L,false)

    }
}


