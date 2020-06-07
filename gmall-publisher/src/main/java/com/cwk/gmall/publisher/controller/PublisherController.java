package com.cwk.gmall.publisher.controller;


import com.alibaba.fastjson.JSON;
import com.cwk.gmall.publisher.service.PublisherService;
import com.cwk.gmall.publisher.service.impl.PublisherServiceImpl;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.*;

@RestController
public class PublisherController {


    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {


        List<Map> totallist = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);
        totallist.add(dauMap);


        Map newmidMap = new HashMap();
        newmidMap.put("id", "newMid");
        newmidMap.put("name", "新增设备");
        newmidMap.put("value", 233);
        totallist.add(newmidMap);
        return JSON.toJSONString(totallist);

    }



    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam("id") String id
    ,@RequestParam("date") String today) {


        if ("dau".equals(id)) {
            //今天
            Map dauHourTDMap = publisherService.getDauHourMap(today);

            //求昨天分时明细
            String yesterDay = getYesterDat(today);
            Map dauHourYDMap = publisherService.getDauHourMap(yesterDay);


            Map hourMap = new HashMap();
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);

            return JSON.toJSONString(hourMap);

        }
        return null;


    }

    private String getYesterDat(String today) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        String yserterday = "";
        try {
            Date Todatdate = simpleDateFormat.parse(today);
            Date yesterDayDate = DateUtils.addDays(Todatdate, -1);
            yserterday = simpleDateFormat.format(yesterDayDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }


        return yserterday;
    }



}
