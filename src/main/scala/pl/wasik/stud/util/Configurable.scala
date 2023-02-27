package pl.wasik.stud.util

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory


trait Configurable {
  val conf: Config = com.typesafe.config.ConfigFactory.load()
}
