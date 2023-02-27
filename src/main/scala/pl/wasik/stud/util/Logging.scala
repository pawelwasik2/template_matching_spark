package pl.wasik.stud.util

import java.util.logging.Logger

trait Logging {
  lazy val log: Logger = Logger.getLogger(getClass.getName)
}
