package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.bean.Option;
import com.atguigu.gmall.gmallpublisher.bean.OptionGroup;
import com.atguigu.gmall.gmallpublisher.bean.SaleDetailInfo;
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


        HashMap orderAmountMap = new HashMap<>();
        orderAmountMap.put("id","orderAmount");
        orderAmountMap.put("name","新增交易额");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);
    }

    @GetMapping("realtime-hour")
    public String getHourToal(@RequestParam("id")String id,@RequestParam("date") String today)
    {
        if ("dau".equals(id))
        {
            HashMap resultMap = new HashMap<>();
            Map dauHourTDMap = publisherService.getDauHourMap(today);
            String yestoday = reduceOneDay(today);
            Map dauHourYDMap = publisherService.getDauHourMap(yestoday);

            resultMap.put("today",dauHourTDMap);
            resultMap.put("yesterday",dauHourYDMap);
            return JSON.toJSONString(resultMap);
        }
        else if ("orderAmount".equals(id))
        {
            HashMap resultMap = new HashMap<>();
            Map orderHourTDMap = publisherService.getOrderAmontHourMap(today);
            String yestoday = reduceOneDay(today);
            Map orderHourYDMap = publisherService.getOrderAmontHourMap(yestoday);

            resultMap.put("today",orderHourTDMap);
            resultMap.put("yesterday",orderHourYDMap);
            return JSON.toJSONString(resultMap);
        }
        return null;
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int pageNo,
                                @RequestParam("size") int pageSize,
                                @RequestParam("keyword")String keyword)
    {
        Map saleDetailMapWithGender = publisherService.getSaleDetailMap(date, keyword, pageNo, pageSize, "user_gender", 2);
        Integer total = (Integer)saleDetailMapWithGender.get("total");
        List<Map> detailList = (List<Map>)saleDetailMapWithGender.get("detail");
        Map aggsMapGender = (Map)saleDetailMapWithGender.get("aggsMap");

        Long femaleCount = (Long) aggsMapGender.getOrDefault("F", 0);
        Long maleCount = (Long) aggsMapGender.getOrDefault("M", 0);

        Double maleRatio = Math.round(maleCount * 1000D / total) / 10D;
        Double femaleRatio = Math.round(femaleCount * 1000D / total) / 10D;
        ArrayList<Option> optionListGender = new ArrayList<>();
        optionListGender.add(new Option("男",maleRatio));
        optionListGender.add(new Option("女",femaleRatio));

        OptionGroup optionGroupGender = new OptionGroup("性别比例", optionListGender);

        //年龄部分
        Map saleDetailMapWithAge = publisherService.getSaleDetailMap(date, keyword, pageNo, pageSize, "user_age", 2);
        Map aggsMapAge = (Map)saleDetailMapWithAge.get("aggsMap");
        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;

        for (Object o : aggsMapAge.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String age = (String) entry.getKey();
            Long ageCount = (Long)entry.getValue();
            if (Integer.parseInt(age)<20)
            {
                age_20count+=ageCount;
            }
            else if (Integer.parseInt(age) >30)
            {
                age30_count+=ageCount;
            }
            else {
                age20_30count+=ageCount;
            }
        }

        Double age_20Ratio = Math.round(age_20count * 1000D / total) / 10D;
        Double age20_30Ratio = Math.round(age20_30count * 1000D / total) / 10D;
        Double age30_Ratio = Math.round(age30_count * 1000D / total) / 10D;
        ArrayList<Option> optionListAge = new ArrayList<>();
        optionListAge.add(new Option("20岁以下",age_20Ratio));
        optionListAge.add(new Option("20岁-30岁",age20_30Ratio));
        optionListAge.add(new Option("30岁以上",age30_Ratio));
        OptionGroup optionGroupAge = new OptionGroup("年龄占比", optionListAge);

        ArrayList<OptionGroup> optionGroups = new ArrayList<>();
        optionGroups.add(optionGroupGender);
        optionGroups.add(optionGroupAge);

        SaleDetailInfo saleDetailInfo = new SaleDetailInfo(total, optionGroups, detailList);


        return JSON.toJSONString(saleDetailInfo);
    }

}
