package com.spark.akka

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.spark.config.{AkkaConfig, ConfigManager}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class Master extends Actor{

    //构造代码块先被执行
    println("master constructor invoked")

    //定义 Map 和 List 分别用来存储 slaves 的信息
    private var slaveMap = new HashMap[String, SlaveInfo]
    private var slaveList = new ListBuffer[SlaveInfo]

    //prestart方法会在构造代码块执行后被调用，并且只被调用一次
    override def preStart() = {
        println("preStart... ...")

        // 设置每个一段时间检查一下, slave的存活状态
        import context.dispatcher
        context.system.scheduler.schedule(0 seconds, AkkaConfig.getInt(ConfigManager.AKKA_MASTER_CHECKOUTTIME) seconds, self, CheckOutTime)
    }

    override def receive: Receive = {

        // 接收slave的注册信息
        case RegisterMsg(slaveId, memory, cores) => {
            //println(s"slave1 $slaveId ,$memory, $cores")
            //1 将注册的slave进行封装
            val slaveInfo = new SlaveInfo(slaveId, memory, cores)

            //2 将slaveInfo信息, 加入到 slaveMap 和 slaveList中
            if (slaveMap.contains(slaveId) == false) {
                slaveMap.put(slaveId, slaveInfo)
                slaveList += slaveInfo

                //3 给slave一个注册成功的反馈信息
                sender ! RegisterSuccessMsg(s"$slaveId 注册成功!")
            }

        }

        // 接收slave的心跳信息, 判断其是否存活
        case SendHeartBeat(slaveId) => {
            //判断worker是否已经注册，master只接受已经注册过的worker的心跳信息
            if(slaveMap.contains(slaveId)) {
                // 设置slave上一次汇报心跳的时间
                val slave: SlaveInfo = slaveMap(slaveId)
                slave.lastHeartTime = System.currentTimeMillis()
            }
        }

        case CheckOutTime => {

            //过滤出死亡的slave
            slaveList.foreach(println(_))
            val diedSlaveList: ListBuffer[SlaveInfo] = slaveList.filter(slave => {
                val time: Long = slave.lastHeartTime;
                val currentTime: Long = System.currentTimeMillis();
                currentTime - time < 10;
            })

            for (slave <- diedSlaveList) {
                if (slaveMap.contains(slave.slaveId)){
                    slaveMap.remove(slave.slaveId)
                    slaveList -= slave
                }
            }

            val liveSlaves: Int = slaveMap.size
            println(s"当前存活的slave个数; $liveSlaves")
        }

    }
}

object Master {
    def main(args: Array[String]): Unit = {

        val host = AkkaConfig.getProperty(ConfigManager.AKKA_MASTER_HOST)
        val port = AkkaConfig.getInt(ConfigManager.AKKA_MASTER_PORT)
        val masterName = AkkaConfig.getProperty(ConfigManager.AKKA_MASTER_NAME)

        //1 创建ActorSystem对象, 它负责创建和监督actor，它是单例对象
        //配置config对象 利用ConfigFactory解析配置文件，获取配置信息
        val configStr =
            s"""
              |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
              |akka.remote.netty.tcp.hostname = "$host"
              |akka.remote.netty.tcp.port = "$port"
            """.stripMargin
        val config: Config = ConfigFactory.parseString(configStr)
        val masterActorSystem = ActorSystem("masterActorSystem", config)

        //2. 通过ActorSystem来创建 master 主节点
        val masterRef: ActorRef = masterActorSystem.actorOf(Props(new Master), masterName)

        //3 发送消息
//        masterRef ! "hekk"


        
        
    }
}
