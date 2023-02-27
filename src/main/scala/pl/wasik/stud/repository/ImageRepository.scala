package pl.wasik.stud.repository

import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.wasik.stud.service.ImageService.spark
import pl.wasik.stud.util.{Configurable, ImageData}

import java.awt.image.BufferedImage
import java.io.File
import java.nio.file.{FileSystems, Files, Paths}
import java.util.UUID
import javax.imageio.ImageIO
import scala.jdk.CollectionConverters.asScalaIteratorConverter

object ImageRepository extends Configurable {
  def listImagesPaths(): List[String] = {
    listFilesPaths(conf.getString("application.config.path.images"))
  }

  def listTemplatesPaths(): List[String] = {
    listFilesPaths(conf.getString("application.config.path.templates"))
  }

  private def listFilesPaths(path: String): List[String] = {
    Files.list(FileSystems.getDefault.getPath(path)).iterator().asScala.map(_.toString).toList
  }

  def listFiles(path: String): List[BufferedImage] = {
    listFilesPaths(path).map(path => ImageIO.read(new File(path)))
  }

  def loadImage(spark: SparkSession, imagePath: String): ImageData ={
    new ImageData(spark.read.format("image").load(imagePath))
  }

  def clearOutputPath(method: String): Unit = {
    val path = conf.getString("application.config.path.output") + System.getProperty("file.separator") + method
    try {
      Files.list(Paths.get(path)).forEach(file => {
        Files.delete(file)
      })
    } catch {
      case _: Throwable => println("Exception occurred during clearing output path")
    }
  }

  def writeImage(image: BufferedImage, method: String): Unit = {
    val path = new File(conf.getString("application.config.path.output")+System.getProperty("file.separator")+method+System.getProperty("file.separator")+UUID.randomUUID+".png")
    ImageIO.write(image, "png", path)
  }
}
