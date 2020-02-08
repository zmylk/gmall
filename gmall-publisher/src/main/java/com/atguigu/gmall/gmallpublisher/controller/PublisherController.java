package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.apache.commons.lang.time.DateUtils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date")String date){

        List<Map> totalList = new ArrayList<>();
        HashMap dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauToal = publisherService.getDauToal(date);
        dauMap.put("value",dauToal);
        totalList.add(dauMap);

        HashMap newMidMap = new HashMap<>();
        newMidMap.put("id","newMid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",233);
        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getHourToal(@RequestParam("id")String id,@RequestParam("date") String today)
    {
        HashMap resultMap = new HashMap<>();
        Map dauHourTDMap = publisherService.getDauHourMap(today);
        String yestoday = reduceOneDay(today);
        Map dauHourYDMap = publisherService.getDauHourMap(yestoday);

        resultMap.put("today",dauHourTDMap);
        resultMap.put("yesterday",dauHourYDMap);
        return JSON.toJSONString(resultMap);
    }

    public String reduceOneDay(String today)
    {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yestoday ="";
        try {
            Date date = simpleDateFormat.parse(today);
            Date reduceDate = DateUtils.addDays(date, -1);
            yestoday = simpleDateFormat.format(reduceDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yestoday;
    }


}
