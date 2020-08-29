package cn.itcast.tags.spark.hbase

import org.apache.hadoop.hbase.filter.CompareFilter

import scala.util.matching.Regex

/**
 * 封装FILTER CLAUSE语句至Condition对象中
 *
 * @param field 字段名称
 * @param compare 字段比较操作，eq、ne、gt、lt、ge和le
 * @param value 字段比较的值
 */
case class Condition(
                        field: String, //
                        compare: CompareFilter.CompareOp, //
                        value: String
                    )

object Condition{

    // 正则表达式
    /*
      "."：匹配除了换行符以外的任何字符
      "*"(贪婪)：重复零次或更多
      "?"(占有)：重复零次或一次
      "( )"：标记一个子表达式的开始和结束位置
     */
    val FULL_REGEX: Regex = "(.*?)\\[(.*?)\\](.*+)".r

    /**
     * 解析Filter Clause，封装到Condition类中
     * @param filterCondition  封装where语句，格式为：modified[GE]20190601 -> (.*?)\[(.*?)\](.*+)
     * @return Condition对象
     */
    def parseCondition(filterCondition : String ) : Condition = {

        // step 1  / 使用正则匹配
        val optionMatch: Option[Regex.Match] = FULL_REGEX.findFirstMatchIn(filterCondition)

        // 2 获取匹配值
        val matchValue: Regex.Match = optionMatch.get

        // step 3 获取比较符 转化类型    eq、ne、gt、lt、ge和le
        val compareValue: CompareFilter.CompareOp = matchValue.group(2).toLowerCase match {
            case "eq" => CompareFilter.CompareOp.EQUAL
            case "ne" => CompareFilter.CompareOp.NOT_EQUAL
            case "gt" => CompareFilter.CompareOp.GREATER
            case "lt" => CompareFilter.CompareOp.LESS
            case "ge" => CompareFilter.CompareOp.GREATER_OR_EQUAL
            case "le" => CompareFilter.CompareOp.LESS_OR_EQUAL
        }

        //step 创建Condition对象，设置属性值
        Condition(
            matchValue.group(1),
            compareValue,
            matchValue.group(3)
        )

    }


}
