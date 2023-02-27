package pl.wasik.stud.util

import pl.wasik.stud.repository.ImageRepository.listFiles

import java.awt.color.ColorSpace
import java.io.File
import javax.imageio.ImageIO

object ImageUtils extends Configurable {
  def convertInputsToGrayscale(): Unit = {
    convertTemplatesToGrayscale()
  }

  def convertTemplatesToGrayscale(): Unit ={
    listFiles(conf.getString("application.config.path.templates")).foreach(image => {
      val isGray = image.getColorModel.getColorSpace.getType == ColorSpace.TYPE_GRAY
      if(isGray) println("gray")
      //println(image.getRGB(0,0).toString)
      for(x <- 0 until image.getWidth) {
        for(y <- 0 until image.getHeight()) {
          var p = image.getRGB(x, y)
          val a = (p >> 24) & 0xff
          val r = (p >> 16) & 0xff
          val g = (p >> 8) & 0xff
          val b = p & 0xff
          val avg = (r + g + b) / 3
          p = (a << 24) | (avg << 16) | (avg << 8) | avg
          image.setRGB(x, y, p)
        }
      }
      ImageIO.write(image, "png", new File(conf.getString("application.config.path.temp_templates")+"test"))
    })
  }

}
