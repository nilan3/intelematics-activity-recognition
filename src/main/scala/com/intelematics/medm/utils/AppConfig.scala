package com.intelematics.medm.utils

import java.io.InputStream
import java.util
import scala.collection.JavaConverters._

import org.yaml.snakeyaml.Yaml

class AppConfig (configInputStream: InputStream) extends Serializable {
  val configMap: java.util.Map[String, Any] = new Yaml().load[java.util.Map[String, Any]](configInputStream)

  def getParameter(parameterPath: String): Any =
    getParameterOption(parameterPath)
      .getOrElse("")

  def getParameterOption(parameterPath: String): Option[Any] = {
    val parameters = parameterPath.split("\\.")
    val (heads, tail) = parameters.splitAt(parameters.length - 1)
    heads.foldLeft(
      Option(configMap)
    )(
      (section, parameter) => section.flatMap(getSection(_, parameter))
    ).flatMap(map => Option(map.get(tail.head)))
  }

  def getSection(configMap: java.util.Map[String, Any], sectionName: String):
  Option[java.util.Map[String, Any]] = {
    Option(configMap.get(sectionName)).map {
      case map: java.util.Map[String, Any] => map
      case _ => throw new YMLConfigException("Params sections " +
        "in yml config file must be compatible with java.util.Map[String,Any] format.")
    }
  }

  def getString(key: String): String = getParameter(key).toString

  def getStringOption(key: String): Option[String] = getParameterOption(key).map(_.toString)

  def getDouble(key: String): Double = getString(key).toDouble

  def getBoolean(key: String): Boolean = getString(key).toBoolean

  def getInt(key: String): Int = getString(key).toInt

  def getList[T](key: String): List[T] = {
    getParameterOption(key) match {
      case value: Option[util.ArrayList[T]] =>
        value.getOrElse(new util.ArrayList[T]()).asScala.toList
      case _ => throw new YMLConfigException("A value in a yaml config must be compatible with List.")
    }
  }

}

class YMLConfigException(s: String) extends RuntimeException(s)
