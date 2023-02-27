package pl.wasik.stud.service

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, broadcast, col, exp, lit, log, max, min, pow, round, sum, sqrt}
import pl.wasik.stud.repository.ImageRepository
import pl.wasik.stud.util.{Coefficient, Configurable, ImageData, Logging, SparkUtils}

import java.awt.Color
import java.awt.image.BufferedImage
import scala.collection.mutable
import scala.util.Try

object ImageService extends Logging with Configurable {

  private val spark = SparkUtils.createSparkSession()
  import spark.implicits._

  def templateMatching(): Unit = {
    val method = "spark"
    ImageRepository.clearOutputPath(method)
    val levels = conf.getInt("application.config.processing.levels")

    ImageRepository.listImagesPaths().foreach(inputPath => {
      val input = createProperImage(ImageRepository.loadImage(spark, inputPath))

      var scaledInputMap = Map(levels -> input)
      for (n <- levels-1 to 1 by -1) {
        scaledInputMap = scaledInputMap + (n -> scaleImage(scaledInputMap(n + 1), 0.5))
      }

      ImageRepository.listTemplatesPaths().foreach(templatePath => {
        //val rotationArray: Array[Int] = Array(0, 90, 180, 270)
        val rotationArray: Array[Int] = Array(0)
        val template = createProperImage(ImageRepository.loadImage(spark, templatePath))

        for(rotationDegree <- rotationArray) {
          val rotatedTemplate = rotateTemplate(template, rotationDegree)

          val matchSetForAllScales: mutable.Set[Coefficient] = mutable.Set()
          //matchSetForAllScales += new Coefficient(0, 0, 0.0, 0, 0)

          val minScale = conf.getDouble("application.config.scale.min")
          val maxScale = conf.getDouble("application.config.scale.max")
          val stepScale = conf.getDouble("application.config.scale.step")

          for(scale <- minScale to maxScale by stepScale) {
            val scaledTemplate = scaleImage(rotatedTemplate, scale)

            var scaledTemplatesMap = Map(levels -> scaledTemplate)
            for (n <- levels-1 to 1 by -1) {
              scaledTemplatesMap = scaledTemplatesMap + (n -> scaleImage(scaledTemplatesMap(n + 1), 0.5))
            }

            crossCorrelationCalculation(scaledInputMap(1), scaledTemplatesMap(1), 1, 0, scaledInputMap(1).w-scaledTemplatesMap(1).w, 0, scaledInputMap(1).h-scaledTemplatesMap(1).h)

            val matchSetForSingleScale: mutable.Set[Coefficient] = mutable.Set()

          }
        }
      })
    })
  }

  private def crossCorrelationCalculation(image: ImageData, template: ImageData, lvl: Int, uMin: Int, uMax: Int, vMin: Int, vMax: Int): mutable.Set[Coefficient] = {
    val pixNum = template.w * template.h
    val meanTemplateColor = calculateColorMean(template)
    val sigmaSquare = calculateSigmaSquare(template, meanTemplateColor)

    val templateW = template.w
    val templateH = template.h
    val matchSet: mutable.Set[Coefficient] = mutable.Set()


    println("pixnum: " + pixNum)
    println("meanTemplateColor: " + meanTemplateColor)
    println("sigmaSquare: " + sigmaSquare)
    val df = image.df
    val tmpl = template.df
    writeImage(image)
    writeImage(template)

    //poprawne
    /*df.limit(2).as("df1")
      .join(df.as("df2"),
        ((col("df1.w") - col("df2.w") <= 0) && col("df1.w") - col("df2.w") >= -2) &&
          ((col("df1.h") - col("df2.h") <= 0) && col("df1.h") - col("df2.h") >= -2),
        "inner"
      )
      /*.groupBy("df1.w", "df1.h")
      .agg(min("df1.color") as "color", sum("df2.color") as "sum")*/
      .orderBy("df1.w", "df1.h")
      .show()*/

    df.select(col("w"), col("h"))
      .where(col("w") >= uMin && col("h") >= vMin && col("w") < uMax && col("h") < vMax)
      .as("df1")
      .join(df.as("df2"),
        ((col("df2.w") >= col("df1.w")) && (col("df2.w") < col("df1.w") + templateW)) &&
          ((col("df2.h") >= col("df1.h")) && (col("df2.h") < col("df1.h") + templateH)),
        "inner"
      )
      .join(tmpl.as("tmpl"),
        (col("tmpl.w") === col("df2.w") - col("df1.w")) &&
        (col("tmpl.h") === col("df2.h") - col("df1.h")),
        "inner"
      )
      .groupBy("df1.w", "df1.h")
      //.agg(sum("df2.color") as "sum", exp(sum(functions.log(col("df2.color")))) as "square_sum", col("") * col("") as "covariance")
      .agg(
        sum("df2.color") as "sum",
        sum(col("df2.color") * col("df2.color")) as "square_sum",
        sum(col("df2.color") * col("tmpl.color")) as "covariance")
      //.withColumn("denumerator", (sqrt(col("square_sum") - ((col("sum") * col("sum")) / pixNum)) * sigmaSquare))
      .withColumn("denumerator", sqrt(col("square_sum") - ((col("sum") * col("sum")) / pixNum)) * sigmaSquare)
      .withColumn("numerator", col("covariance") - (col("sum") * meanTemplateColor))
      //.withColumn("coef", (sqrt(col("square_sum") - ((col("sum") * col("sum")) / pixNum)) * sigmaSquare) / (col("covariance") - col("sum") * meanTemplateColor))
      .withColumn("coef", col("numerator") / col("denumerator"))
      .select("df1.w", "df1.h", "coef")
      //.orderBy("df1.w", "df1.h", "df2.w", "df2.h", "tmpl.w", "tmpl.h")
      .orderBy("df1.w", "df1.h")
      .show(200)

    /*double denumerator = Math.sqrt((squareSum - ((sum * sum) / pixNum))) * sigmaSquare;
    double numerator = (covariance - sum * meanOfTemplate);*/

    /*df.as("df1")
      .join(df.as("df2"),
        ((col("df1.w") - col("df2.w") <= 1) && col("df1.w") - col("df2.w") >= -1) &&
          ((col("df1.h") - col("df2.h") <= 1) && col("df1.h") - col("df2.h") >= -1),
        "inner"
      )
      /*.groupBy("df1.w", "df1.h")
      .agg(min("df1.color") as "color", sum("df2.color") as "sum")*/
      .orderBy("df1.w", "df1.h")
      .show()*/

    /*val matchSet: mutable.Set[Coefficient] = mutable.Set()
    val imageDf = image.df*/
    // && col("h") > vMin && col("w") < uMax && col("h") < vMax



    /*val windowW = Window.rangeBetween(Window.currentRow, Window.currentRow + num1)
    val windowH = Window.rangeBetween(Window.currentRow, Window.currentRow + num2)
    Window.unboundedFollowing

    df.withColumn("color_sum", sum(col("color")).over(col("w").windowW and col("h").windowH))

    image.df.where(col("w") >= uMin && col("h") >= vMin && col("w") < uMax && col("h") < vMax)
      .withColumn("color_sum", sum(col("color")))

    image.df.where(col("w") >= uMin && col("h") >= vMin && col("w") < uMax && col("h") < vMax).map(row => {
      val wMax = row.getInt(0) + templateW
      val hMax = row.getInt(1) + templateH
      val colorSum = imageDf.where(col("w")>row(0) && col("w")<wMax && col("h") > row(1) && col("w") < hMax).agg(sum(col("color")))
      (row.getInt(0), row.getInt(1), row.getDouble(2))
    }).show()*/

    //select values from image or template where h, w <> row(3)


      //.withColumn("sum", sum(when()))


    matchSet
  }

  private def calculateSigmaSquare(image: ImageData, meanOfImage: Double): Double = {
    Math.sqrt(image.df.withColumn("square", pow(col("color") - meanOfImage, 2)).agg(sum(col("square"))).head().getDouble(0))
  }

  private def calculateColorMean(image: ImageData): Double = {
    image.df.select(col("color")).agg(avg("color").alias("avg")).head().getDouble(0)
  }

  private def rotateTemplate(template: ImageData, degree: Int): ImageData = {
    degree match {
      case 0 => template
      case 90 => rotate90Degree(template)
      case 180 => rotate180Degree(template)
      case 270 => rotate270Degree(template)
    }
  }

  private def rotate90Degree(template: ImageData): ImageData = {
    val width = template.w
    val height = template.h

    new ImageData(Array.range(0, height).toSeq.toDF("targetW")
      .crossJoin(Array.range(0, width).toSeq.toDF("targetH"))
      .withColumn("sourceW", -col("targetH") - 1 + width)
      .withColumn("sourceH", col("targetW"))
      .join(broadcast(template.df), col("sourceW")===template.df("w") && col("sourceH")===template.df("h"))
      .drop("sourceW", "sourceH", "w", "h")
      .withColumnRenamed("targetW", "w")
      .withColumnRenamed("targetH", "h"),
      height, width)
  }

  private def rotate180Degree(template: ImageData): ImageData = {
    val width = template.w
    val height = template.h

    new ImageData(Array.range(0, width).toSeq.toDF("targetW")
      .crossJoin(Array.range(0, height).toSeq.toDF("targetH"))
      .withColumn("sourceW", -col("targetW") - 1 + width)
      .withColumn("sourceH", -col("targetH") - 1 + height)
      .join(broadcast(template.df), col("sourceW")===template.df("w") && col("sourceH")===template.df("h"))
      .drop("sourceW", "sourceH", "w", "h")
      .withColumnRenamed("targetW", "w")
      .withColumnRenamed("targetH", "h"),
      width, height)
  }

  private def rotate270Degree(template: ImageData): ImageData = {
    val width = template.w
    val height = template.h

    new ImageData(Array.range(0, height).toSeq.toDF("targetW")
      .crossJoin(Array.range(0, width).toSeq.toDF("targetH"))
      .withColumn("sourceW", col("targetH"))
      .withColumn("sourceH", -col("targetW") - 1 + height)
      .join(broadcast(template.df), col("sourceW")===template.df("w") && col("sourceH")===template.df("h"))
      .drop("sourceW", "sourceH", "w", "h")
      .withColumnRenamed("targetW", "w")
      .withColumnRenamed("targetH", "h"),
      height, width)
  }

  private def scaleImage(imageData: ImageData, scale: Double): ImageData ={
    val width = imageData.w
    val height = imageData.h
    val targetWidth = (width * scale).toInt
    val targetHeight = (height * scale).toInt
    val xArray = Array.range(0, targetWidth)
    val yArray = Array.range(0, targetHeight)

    val df = xArray.toSeq.toDF("targetW").crossJoin(yArray.toSeq.toDF("targetH"))
      .withColumn("sourceW", round(col("targetW") / targetWidth * width).cast("int"))
      .withColumn("sourceH", round(col("targetH") / targetHeight * height).cast("int"))
      .join(broadcast(imageData.df), col("sourceW")===imageData.df("w") && col("sourceH")===imageData.df("h"), "left")
      .drop("sourceW", "sourceH", "w", "h")
      .withColumnRenamed("targetW", "w")
      .withColumnRenamed("targetH", "h")

    new ImageData(df, targetWidth, targetHeight)
  }

  private def writeImage(image: ImageData): Unit = {
    val w = image.w
    val h = image.h
    val out = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY)

    val imageTuples = image.df.collectAsList()
    imageTuples.forEach(row => {
      val grayscale = row.getDouble(2).toInt
      out.setRGB(row.getInt(0), row.getInt(1), new Color(grayscale, grayscale, grayscale).getRGB)
    })
    ImageRepository.writeImage(out, "spark")
  }

  private def createProperImage(imageData: ImageData): ImageData = {
    val imageByteArray = imageData.df.select(col("image.data")).rdd.flatMap(f => {
      f.getAs[Array[Byte]]("data")
    })

    val width = imageData.w
    val height = imageData.h

    val channelOffSet = spark.sparkContext.longAccumulator("channelOffSetAcc")
    val x = spark.sparkContext.longAccumulator("xAcc")
    val y = spark.sparkContext.longAccumulator("yAcc")
    var channels = imageData.df.select("image.nChannels").head().getInt(0)

    import spark.implicits._

    var imageByte = imageByteArray.zipWithIndex()
    if (channels == 4) {
      imageByte = imageByte.filter(f => (f._2 % 4) != 3)
      channels = channels - 1
    }

    val df = imageByte.map(f => {
      if(channelOffSet.value == channels) {
        channelOffSet.reset()
        x.add(1)
      }
      channelOffSet.add(1)

      if (x.value == width) {
        x.reset()
        y.add(1)
      }
      (f._1 & 0xFF, x.value, y.value)
    }).toDF
      .withColumnRenamed("_1", "color")
      .withColumnRenamed("_2", "w")
      .withColumnRenamed("_3", "h")
      .groupBy(col("w"), col("h")).agg(avg("color").as("color"))
      .orderBy("w", "h")
      .withColumn("w", col("w").cast("int"))
      .withColumn("h", col("h").cast("int"))
    new ImageData(df, width, height)
  }

  /*def parseBitMap(): List[Int, Int] = {

  }*/

  def getImageParameter(image: DataFrame, parameter: String): Int = {
    parameter match {
      case "w" => if (Try(image(parameter)).isSuccess) image.select(max(col(parameter))).head().getInt(0) + 1
        else image.select(col("image.width")).head().getInt(0)
      case "h" => if (Try(image(parameter)).isSuccess) image.select(max(col(parameter))).head().getInt(0) + 1
      else image.select(col("image.height")).head().getInt(0)
    }
  }
}
