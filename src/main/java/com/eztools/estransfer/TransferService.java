package com.eztools.estransfer;

import com.eztools.estransfer.common.ESSourceClient;
import com.eztools.estransfer.common.ESTargetClient;
import com.eztools.estransfer.common.EsSourceConfig;
import com.eztools.estransfer.common.EsTargetConfig;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 转换服务类
 * Created by liChenYu on 2018/6/17
 */
@Service
public class TransferService {
    private static Logger s_logger = LoggerFactory.getLogger(TransferService.class);

    private static TimeValue timeoutValue = new TimeValue(60, TimeUnit.SECONDS);

    private static final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

    @Value("${es.size}")
    int size = 1000;

    public void doTransfer(EsSourceConfig esSourceConfig, EsTargetConfig esTargetConfig) {
        RestHighLevelClient sourceHighClient = ESSourceClient.getHightClient(esSourceConfig);
        RestHighLevelClient targetHighClient = ESTargetClient.getHightClient(esTargetConfig);

        String[] fetchSource = null;
        if(esSourceConfig.getProperties().length > 0
                && !esSourceConfig.getProperties()[0].isEmpty()
                && !esSourceConfig.getProperties()[0].equals("*")) {
            fetchSource = esSourceConfig.getProperties();
        }
        String scrollId = "";
        SearchResponse searchResponse = null;
        int currentTotal = 0;
        while (true) {
            long startTime = System.currentTimeMillis();
            //1. get from source
            if(scrollId.isEmpty()) {
                try {
                    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                    sourceBuilder.parseXContent(getQuery(esSourceConfig));
                    sourceBuilder.size(size).fetchSource(fetchSource, null).timeout(timeoutValue);

                    SearchRequest searchRequest = new SearchRequest(esSourceConfig.getIndex())
                            .types(esSourceConfig.getType())
                            .source(sourceBuilder).scroll(scroll);

                    searchResponse = sourceHighClient.search(searchRequest);
                    scrollId = searchResponse.getScrollId();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
                try {
                    searchResponse = sourceHighClient.searchScroll(scrollRequest);
                    scrollId = searchResponse.getScrollId();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            //2. send to target
            currentTotal = sendToTarget(targetHighClient, searchResponse.getHits(), esTargetConfig, startTime, currentTotal);

            // end
            if(searchResponse.getHits().getHits().length < size) {
                s_logger.info("data transfer finished!");
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                try {
                    sourceHighClient.clearScroll(clearScrollRequest);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                break;
            }
        }
    }

    private QueryParseContext getQuery(EsSourceConfig esSourceConfig) {
        String query = esSourceConfig.getQuery();
        query = "{\"query\":" + query + "}";
        QueryParseContext queryParseContext = null;

        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), query);
            queryParseContext = new QueryParseContext(parser);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queryParseContext;
    }

    private int sendToTarget(RestHighLevelClient targetHighClient,
                             SearchHits searchHits,
                             EsTargetConfig esTargetConfig,
                             long startTime,
                             int currentTotal) {
        BulkRequest bulkRequest = new BulkRequest();
        for(SearchHit searchHit : searchHits) {
            IndexRequest indexRequest = new IndexRequest(esTargetConfig.getIndex(),
                    esTargetConfig.getType(),
                    searchHit.getId()).source(searchHit.getSource());
            UpdateRequest updateRequest = new UpdateRequest(esTargetConfig.getIndex(),
                    esTargetConfig.getType(),
                    searchHit.getId()).doc(searchHit.getSource()).upsert(indexRequest);

            bulkRequest.add(updateRequest);
        }

        int doneSize = bulkRequest.numberOfActions();
        try {
            BulkResponse bulkResponse = targetHighClient.bulk(bulkRequest);
            for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                if(bulkItemResponse.isFailed()) {
                    doneSize--;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            doneSize = 0;
        }
        currentTotal += doneSize;
        s_logger.info("write to target : " + doneSize +
                " , take time : " + (System.currentTimeMillis() - startTime) + "ms" +
                " , total : " + currentTotal);
        return currentTotal;
    }
}
