package com.spark.akka

class SlaveInfo(val slaveId: String, val memory: Int, val cores: Int) {

    // 默认的初始心跳时间
    var lastHeartTime: Long = _

    override def toString: String= {
        s"slaveInfo: $slaveId, $memory, $cores"
    }

}
