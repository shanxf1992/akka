package com.spark.config

import java.io.InputStream
import java.util.Properties

object AkkaConfig {

    private val resourceAsStream: InputStream = AkkaConfig.getClass.getClassLoader.getResourceAsStream("config.properties")
    private val props = new Properties
    props.load(resourceAsStream)

    /**
      * 从配置文件中获取String 类型的变量
      * @param name
      * @return
      */
    def getProperty(name: String): String = {
        return props.getProperty(name)
    }

    /**
      * 从配置文件中获取Int 类型的变量
      * @param name
      * @return
      */
    def getInt(name: String): Int = {
        return props.getProperty(name).toInt
    }
}
