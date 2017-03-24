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

package com.navercorp.pinpoint.collector.receiver;

import org.apache.thrift.TBase;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.navercorp.pinpoint.collector.handler.EsAgentInfoHandler;
import com.navercorp.pinpoint.collector.handler.EsApiMetaDataHandler;
import com.navercorp.pinpoint.collector.handler.EsSqlMetaDataHandler;
import com.navercorp.pinpoint.collector.handler.EsStringMetaDataHandler;
import com.navercorp.pinpoint.collector.handler.RequestResponseHandler;
import com.navercorp.pinpoint.collector.handler.SimpleHandler;
import com.navercorp.pinpoint.thrift.dto.TAgentInfo;
import com.navercorp.pinpoint.thrift.dto.TApiMetaData;
import com.navercorp.pinpoint.thrift.dto.TSqlMetaData;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;

/**
 * @author yangjian
 */
public class EsTcpDispatchHandler extends AbstractDispatchHandler {

    @Autowired
    private EsAgentInfoHandler esAgentInfoHandler;

    @Autowired
    private EsSqlMetaDataHandler esSqlMetaDataHandler;

    @Autowired
    private EsApiMetaDataHandler esApiMetaDataHandler;

    @Autowired
    private EsStringMetaDataHandler esStringMetaDataHandler;



    public EsTcpDispatchHandler() {
        this.logger = LoggerFactory.getLogger(this.getClass());
    }


    @Override
    RequestResponseHandler getRequestResponseHandler(TBase<?, ?> tBase) {
        if (tBase instanceof TSqlMetaData) {
            return esSqlMetaDataHandler;
        }
        if (tBase instanceof TApiMetaData) {
            return esApiMetaDataHandler;
        }
        if (tBase instanceof TStringMetaData) {
            return esStringMetaDataHandler;
        }
        if (tBase instanceof TAgentInfo) {
            return esAgentInfoHandler;
        }
        return null;
    }

    @Override
    SimpleHandler getSimpleHandler(TBase<?, ?> tBase) {

        if (tBase instanceof TAgentInfo) {
            return esAgentInfoHandler;
        }

        return null;
    }
}
