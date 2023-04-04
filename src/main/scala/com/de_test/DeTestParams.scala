package com.de_test

case class DeTestParams(
                         productNamesPath:String = "",
                         dateFrom: String = "",
                         dateTo: String = "",
                         kktCategory: String = "",
                         needGroupByReceiptDate: Boolean = false,
                         needGroupByRegion: Boolean = false,
                         needGroupByChannel: Boolean = false
                       )

object DeTestParams {

  def apply(args: Seq[String]): Option[DeTestParams] = {

    val parser = new scopt.OptionParser[DeTestParams]("Parsing application args") {

      opt[String]('p', "product-names-path").required().action((x, c) =>
        c.copy(productNamesPath = x)).text("path of product_names csv")

      opt[String]('f', "date-from").required().action((x, c) =>
        c.copy(dateFrom = x)).text("date_from")

      opt[String]('t', "date-to").required().action((x, c) =>
        c.copy(dateTo = x)).text("date_to")

      opt[String]('k', "kkt-category").optional().action((x, c) =>
        c.copy(kktCategory = x)).text("kkt_category")

      opt[Boolean]('d', "receipt-date-group").optional().action((x, c) =>
        c.copy(needGroupByReceiptDate = x)).text("is need group by receipt_date")

      opt[Boolean]('c', "channel-group").optional().action((x, c) =>
        c.copy(needGroupByChannel = x)).text("is need group by channel_group")

      opt[Boolean]('r', "region-group").optional().action((x, c) =>
        c.copy(needGroupByRegion = x)).text("is need group by region_group")

    }


    parser.parse(args, DeTestParams()) match {
      case Some(config) => Some(config)

      case None => throw new IllegalArgumentException("wrong arguments")
    }
  }
}


