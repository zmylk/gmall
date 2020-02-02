package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.constant.GmallConstants;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class PublisherServiceImpl implements PublisherService {


    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauToal(String date) {

        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2020-02-03\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}\n";

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
}
