package com.intelematics.medm.utils

import java.io.FileInputStream

object Utilities {

  def loadConfig(args: Array[String]): AppConfig = {
    if (args.length > 0) {
      val config_file = args(0)
      val config = new AppConfig(new FileInputStream(config_file))
      config
    } else {
      print("Yml configuration file isn't defined")
      sys.exit(1)
    }
  }

}
