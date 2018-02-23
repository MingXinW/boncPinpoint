package com.navercorp.pinpoint.collector.dao.es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.StringMetaDataDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.bo.StringMetaDataBo;
import com.navercorp.pinpoint.thrift.dto.TStringMetaData;

@Repository("esStringMetaDataDao")
public class ESStringMetaDataDao implements StringMetaDataDao {

	 private final Logger logger = LoggerFactory.getLogger(this.getClass());
	 
	@Override
	public void insert(TStringMetaData stringMetaData) {
		// TODO Auto-generated method stub

		if (stringMetaData == null) {
            throw new NullPointerException("stringMetaData must not be null");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("insert:{}", stringMetaData);
        }

        final StringMetaDataBo stringMetaDataBo = new StringMetaDataBo(stringMetaData.getAgentId(), stringMetaData.getAgentStartTime(), stringMetaData.getStringId());
        
        String id = stringMetaDataBo.getAgentId() + EsIndexs.ID_SEP + stringMetaDataBo.getStartTime() + EsIndexs.ID_SEP + stringMetaDataBo.getStringId();
        
        try {
			EsClient.client().prepareIndex(EsIndexs.STRING_METADATA, EsIndexs.TYPE,id).setSource(
					jsonBuilder()
			        .startObject()
			        .field("stringValue", stringMetaData.getStringValue())
			    .endObject()).get();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}

}
