package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {


    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauToal(String date) {
//
//        String query = "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"logDate\": \"2020-02-03\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}\n";

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        sourceBuilder.query(boolQueryBuilder);
        System.out.println(sourceBuilder.toString());
        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0 ;
        try {
            SearchResult execute = jestClient.execute(search);
            total = execute.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  total;
    }

    @Override
    public Map getDauHourMap(String date) {
        //过滤
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        sourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder builder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        sourceBuilder.aggregation(builder);


        Search search = new Search.Builder(sourceBuilder.toString()).addIndex(GmallConstants.ES_INDEX_DAU).addType("_doc").build();
        Map resultMap = new HashMap();
        try {
            SearchResult execute = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = execute.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                resultMap.put(bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }
}
