package com.de_test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

object DeTestApp extends App {

  val appParams = DeTestParams.apply(args).get

  val dateFrom = appParams.dateFrom
  val dateTo = appParams.dateTo
  val productNamesPath = appParams.productNamesPath

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("de_test")
    .getOrCreate();

  import spark.implicits._

  val sqlQuery =
    s"""(select s.product_name_hash, s.kkt_number, version, s.org_inn, shop_id, region, receipt_date, category,
       |total_sum, receipt_date_min, receipt_date_max from sales as s
       |inner join kkt_info as ki on  s.kkt_number = ki.kkt_number
       |inner join kkt_categories as kc on s.kkt_number = kc.kkt_number
       |inner join kkt_activity as ka on s.kkt_number = ka.kkt_number
       |where receipt_date >= '$dateFrom'
       |and receipt_date <= '$dateTo'
       |and ki.date_till = '4000-01-01') df;""".stripMargin

  val productNamesDF = spark
    .read
    .option("header", true)
    .csv("./src/main/resources/product_names.csv")

  val window = Window
    .partitionBy("kkt_number")
    .orderBy(desc("version"))

  val aggDbInfoDF = spark.read.format("jdbc")
    .options(
      Map("driver" -> "org.sqlite.JDBC", "dbtable" -> sqlQuery, "url" -> "jdbc:sqlite:./src/main/resources/de_test.db")
    )
    .load()
    .withColumn("is_active_kkt",
      when('receipt_date_min <= dateFrom or 'receipt_date_max >= dateTo, 1)
        .otherwise(0)
    )
    .withColumn("rn", row_number().over(window))
    .filter('is_active_kkt === 1 and 'rn === 1)

  val interDF = doProcess(appParams, productNamesDF, aggDbInfoDF).cache()

  val all_total_summ = interDF
    .select(sum('total_sum)).first.getLong(0)

  interDF
    .withColumn("total_sum_pct", lit('total_sum / all_total_summ).cast(DecimalType(10, 2)))
    .repartition(1)
    .write
    .option("header", true)
    .csv("report/")


  def doProcess(appParams: DeTestParams, productNamesDF: DataFrame, aggDbInfoDF: DataFrame): DataFrame = {

    val kktCategory = appParams.kktCategory
    val needGroupByReceiptDate = appParams.needGroupByReceiptDate
    val needGroupByRegion = appParams.needGroupByRegion
    val needGroupByChannel = appParams.needGroupByChannel

    val groupCols = Map(
      "reseipt_date" -> needGroupByReceiptDate,
      "region" -> needGroupByRegion,
      "channel" -> needGroupByChannel
    ).filter(_._2).keys.toList

    (appParams.kktCategory.nonEmpty, needGroupByChannel) match {
      case (true, true) =>
        aggDbInfoDF
          .groupBy('org_inn).agg(countDistinct('shop_id).as("shop_cnt"))
          .withColumn("channel", when('shop_cnt >= 3, "chain").otherwise("nonchain"))
          .select('org_inn, 'channel)
          .join(aggDbInfoDF, Seq("org_inn"), "inner")
          .filter('kkt_category === kktCategory)
          .join(productNamesDF, Seq("product_name_hash"), "inner")
          .groupBy("brand", groupCols: _*)
          .agg(sum('total_sum).as("total_sum"))


      case (false, true) =>
        aggDbInfoDF
          .groupBy('org_inn).agg(countDistinct('shop_id).as("shop_cnt"))
          .withColumn("channel", when('shop_cnt >= 3, "chain").otherwise("nonchain"))
          .select('org_inn, 'channel)
          .join(aggDbInfoDF, Seq("org_inn"), "inner")
          .join(productNamesDF, Seq("product_name_hash"), "inner")
          .groupBy("brand", groupCols: _*)
          .agg(sum('total_sum).as("total_sum"))

      case _ =>
        aggDbInfoDF
          .join(productNamesDF, Seq("product_name_hash"), "inner")
          .groupBy("brand", groupCols: _*)
          .agg(sum('total_sum).as("total_sum"))
    }

  }

}
