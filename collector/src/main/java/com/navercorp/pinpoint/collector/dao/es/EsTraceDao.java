/*
 * Copyright 2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.navercorp.pinpoint.collector.dao.es;

import static com.navercorp.pinpoint.common.hbase.HBaseTables.TRACES;
import static com.navercorp.pinpoint.common.hbase.HBaseTables.TRACES_CF_TERMINALSPAN;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.client.Put;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.TracesDao;
import com.navercorp.pinpoint.collector.dao.hbase.filter.SpanEventFilter;
import com.navercorp.pinpoint.collector.util.EsTables;
import com.navercorp.pinpoint.collector.util.JsonUtils;
import com.navercorp.pinpoint.common.bo.SpanBo;
import com.navercorp.pinpoint.common.bo.SpanEventBo;
import com.navercorp.pinpoint.common.util.BytesUtils;
import com.navercorp.pinpoint.common.util.SpanUtils;
import com.navercorp.pinpoint.thrift.dto.TAnnotation;
import com.navercorp.pinpoint.thrift.dto.TSpan;
import com.navercorp.pinpoint.thrift.dto.TSpanChunk;
import com.navercorp.pinpoint.thrift.dto.TSpanEvent;

/**
 * @author yangjian
 */
@Repository
public class EsTraceDao implements TracesDao{

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    
    @Autowired  
    private Client client;
    
    @Autowired
    private SpanEventFilter spanEventFilter;
    
    @Override
    public void insert(final TSpan span) {
        if (span == null) {
            throw new NullPointerException("span must not be null");
        }
        SpanBo spanBo = new SpanBo(span);
        IndexResponse response = client.prepareIndex(EsTables.TRACES,EsTables.TRACES_CF_SPAN).setSource(JsonUtils.encode(spanBo)).execute().actionGet();  
        debugInsert(response);

        List<TAnnotation> annotations = span.getAnnotations();
        if (CollectionUtils.isNotEmpty(annotations)) {
        	response = client.prepareIndex(EsTables.TRACES,EsTables.TRACES_CF_ANNOTATION).setSource(JsonUtils.encode(annotations)).execute().actionGet();
        	debugInsert(response);
        }

        List<TSpanEvent> spanEventBoList = span.getSpanEventList();
        if (CollectionUtils.isNotEmpty(spanEventBoList)) {
        	for (TSpanEvent spanEvent : spanEventBoList) {
                SpanEventBo spanEventBo = new SpanEventBo(span, spanEvent);
                if (spanEventFilter.filter(spanEventBo)) {
                	response = client.prepareIndex(EsTables.TRACES,EsTables.TRACES_CF_TERMINALSPAN).setSource(JsonUtils.encode(spanEventBo)).execute().actionGet();
                	debugInsert(response);
                }
            }
        }
    }
    
    private void debugInsert(IndexResponse response){
    	String index = response.getIndex();  
        String type = response.getType();  
        String id = response.getId();  
        long version = response.getVersion();  
        boolean created = response.isCreated();  
        logger.debug("EsTraceDao insert:"+index+","+type+","+id+","+version+","+created);  
    }
    

    @Override
    public void insertSpanChunk(TSpanChunk spanChunk) {
    	if (spanChunk == null) {
            throw new NullPointerException("spanChunk must not be null");
        }
        final List<TSpanEvent> spanEventBoList = spanChunk.getSpanEventList();
        if (CollectionUtils.isNotEmpty(spanEventBoList)) {
        	for (TSpanEvent spanEvent : spanEventBoList) {
                SpanEventBo spanEventBo = new SpanEventBo(spanChunk, spanEvent);
                if (spanEventFilter.filter(spanEventBo)) {
                	IndexResponse response = client.prepareIndex(EsTables.TRACES,EsTables.TRACES_CF_TERMINALSPAN).setSource(JsonUtils.encode(spanEventBo)).execute().actionGet();
                	debugInsert(response);
                }
            }
        }
    }

}
