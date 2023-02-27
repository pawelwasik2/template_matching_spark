package pl.wasik.stud

import pl.wasik.stud.service.ImageService
import pl.wasik.stud.util.{Configurable, Logging}

object Main extends Configurable with Logging{
  def main(args: Array[String]): Unit = {
    log.info("Starting application...")

    //Sprobowac mozna zapis obrazu z 1 channelem
    //convertInputsToGrayscale()

    ImageService.templateMatching()

  }
}
