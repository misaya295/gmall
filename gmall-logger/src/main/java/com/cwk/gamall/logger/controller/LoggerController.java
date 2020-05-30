package com.cwk.gamall.logger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cwk.gmall.commom.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
public class LoggerController {


    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);
    @PostMapping("/log")
    public String dolong(@RequestParam("log")String logJson){


       /*
       补时间戳
        */
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());

        /*
         * 落盘到log文件  log4j
        */
        logger.info(jsonObject.toJSONString());


        /*
         * 发送kafka
         */
        if ("startup".equals(jsonObject.getString("type"))) {
            System.out.println("发数据给kafka");

            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
            System.out.println("发数据给kafka"+jsonObject.toJSONString());

        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());

        }


        return "success";




    }


}
