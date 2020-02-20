#!/usr/bin/env bash
case  $i in{
       "start"){
        for i in hadoop104
        do
        echo "------------Æô¶¯ $iÏû·Ñ----------------"
        ssh $i "nohup /opt/module/flume/bin/flume-ng agent --conf-file /opt/module/flume/conf/kafka-flume-hdfs.conf  --name a1
            -Dflume.root.logger=INFO,LOGFILE >/opt/module/flume/log.txt 2>&1 &"
            done
      };;

}