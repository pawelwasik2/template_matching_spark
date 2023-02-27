package pl.wasik.stud.util

import org.apache.spark.sql.DataFrame
import pl.wasik.stud.service.ImageService

class ImageData(val df: DataFrame, val w: Int, val h: Int) {

  def this(df: DataFrame) {
    this(df, ImageService.getImageParameter(df, "w"), ImageService.getImageParameter(df, "h"))
  }

  override def toString: String = {
    "image width: " + w + " height: " + h
  }
}
