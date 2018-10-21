package com.spark.akka

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.spark.config.{AkkaConfig, ConfigManager}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

/**
  * slave1
  *     slave 需要做的事情
  *         1. 刚启动时, 需要向master进行注册
  *         2. 每隔一段时间就发送一次心跳信息, 向 master 表示自己存活的状态
  */
class Slaves1(slaveId: String, memory: Int, cores: Int) extends Actor{

    val host = AkkaConfig.getProperty(ConfigManager.AKKA_MASTER_HOST)
    val port = AkkaConfig.getInt(ConfigManager.AKKA_MASTER_PORT)
    val masterName = AkkaConfig.getProperty(ConfigManager.AKKA_MASTER_NAME)

    //构造代码块先被执行
    println("master constructor invoked")

    var masterRef: ActorSelection = _
    //prestart方法会在构造代码块执行后被调用，并且只被调用一次
    // 启动时需要向主节点进行注册, 并且汇报自己的状态
    override def preStart() = {
        //获取master actor的引用
        //ActorContext全局变量，可以通过在已经存在的actor中，寻找目标actor
        //调用对应actorSelection方法，
        // 方法需要一个path路径：1、通信协议、2、master的IP地址、3、master的端口 4、创建master actor老大 5、actor层级
        masterRef = context.actorSelection(s"akka.tcp://masterActorSystem@$host:$port/user/$masterName")
        masterRef ! RegisterMsg(slaveId, memory, cores)
    }

    //receive方法会在prestart方法执行后被调用，不断的接受消息
    override def receive: Receive = {
        //1 接收master的反馈
        case RegisterSuccessMsg(msg) => {

            println(msg)

            //2 定时向master发送心跳信息
            import context.dispatcher
            context.system.scheduler.schedule(0 seconds, AkkaConfig.getInt(ConfigManager.AKKA_SLAVE_HEAT) seconds, self, HeartBeat)
        }

        // 用来发送心跳信息
        case HeartBeat => {
            masterRef ! SendHeartBeat(slaveId)
        }

    }
}

object Slaves1{
    val host = AkkaConfig.getProperty(ConfigManager.AKKA_SLAVE1_HOST)
    val port = AkkaConfig.getInt(ConfigManager.AKKA_SLAVE1_PORT)
    val masterName = AkkaConfig.getProperty(ConfigManager.AKKA_SLAVE1_NAME)

    def main(args: Array[String]): Unit = {

        val slaveId = UUID.randomUUID().toString

        //1 创建 ActorSystem对象
        val configStr =
            s"""
              |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
              |akka.remote.netty.tcp.hostname = "$host"
              |akka.remote.netty.tcp.port = "$port"
            """.stripMargin
        val config: Config = ConfigFactory.parseString(configStr)
        val slaveActorSystem = ActorSystem("slave1", config)

        // 2、通过actorSystem来创建 slave1 actor
        val slave1: ActorRef = slaveActorSystem.actorOf(Props(new Slaves1(slaveId, 65535, 10)), masterName)

//        slave1 ! "self"

    }
}