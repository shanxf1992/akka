package com.spark.akka

trait RemoteMsg extends Serializable{

}

// 样例类 RegisterMsg 用来发送 注册信息
case class RegisterMsg(val slaveId: String, val memory: Int, val cores: Int) extends RemoteMsg

// 用来发送心跳信息
object HeartBeat

// 用来发送注册成功的信息
case class RegisterSuccessMsg(msg: String)

//用来发送心跳信息
case class SendHeartBeat(slaveId: String)

//用来检查slave的状态
object CheckOutTime