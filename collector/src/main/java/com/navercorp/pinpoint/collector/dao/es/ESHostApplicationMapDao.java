package com.navercorp.pinpoint.collector.dao.es;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.navercorp.pinpoint.collector.dao.HostApplicationMapDao;
import com.navercorp.pinpoint.collector.dao.es.base.EsClient;
import com.navercorp.pinpoint.collector.util.AtomicLongUpdateMap;
import com.navercorp.pinpoint.collector.util.EsIndexs;
import com.navercorp.pinpoint.common.server.util.AcceptedTimeService;
import com.navercorp.pinpoint.common.util.TimeSlot;

@Repository("esHostApplicationMapDao")
public class ESHostApplicationMapDao implements HostApplicationMapDao {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private AcceptedTimeService acceptedTimeService;

	@Autowired
	private TimeSlot timeSlot;

	// FIXME should modify to save a cachekey at each 30~50 seconds instead of
	// saving at each time
	private final AtomicLongUpdateMap<CacheKey> updater = new AtomicLongUpdateMap<>();

	@Override
	public void insert(String host, String bindApplicationName, short bindServiceType, String parentApplicationName,
			short parentServiceType) {
		// TODO Auto-generated method stub

		if (host == null) {
			throw new NullPointerException("host must not be null");
		}
		if (bindApplicationName == null) {
			throw new NullPointerException("bindApplicationName must not be null");
		}

		final long statisticsRowSlot = getSlotTime();

		final CacheKey cacheKey = new CacheKey(host, bindApplicationName, bindServiceType, parentApplicationName,
				parentServiceType);
		final boolean needUpdate = updater.update(cacheKey, statisticsRowSlot);
		if (needUpdate) {
			insertHostVer2(host, bindApplicationName, bindServiceType, statisticsRowSlot, parentApplicationName,
					parentServiceType);
		}
	}
	
	private void insertHostVer2(String host, String bindApplicationName, short bindServiceType, long statisticsRowSlot, String parentApplicationName, short parentServiceType) {
        if (logger.isDebugEnabled()) {
            logger.debug("Insert host-application map. host={}, bindApplicationName={}, bindServiceType={}, parentApplicationName={}, parentServiceType={}",
                    host, bindApplicationName, bindServiceType, parentApplicationName, parentServiceType);
        }
        
        String id = parentApplicationName + EsIndexs.ID_SEP + parentServiceType + EsIndexs.ID_SEP + statisticsRowSlot;

        try {
			EsClient.client().prepareIndex(EsIndexs.HOST_APPLICATION_MAP_VER2, EsIndexs.TYPE,id).setSource(
					jsonBuilder()
			        .startObject()
			        .field("host", host)
			        .field("bindApplicationName", bindApplicationName)
			        .field("bindServiceType", bindServiceType)
			    .endObject()).get();
		} catch (IOException ex) {
			 logger.error("retry one. Caused:{}", ex.getCause(), ex);
			 
		}
    }
	
	private long getSlotTime() {
        final long acceptedTime = acceptedTimeService.getAcceptedTime();
        return timeSlot.getTimeSlot(acceptedTime);
    }
	
	private static final class CacheKey {
        private final String host;
        private final String applicationName;
        private final short serviceType;

        private final String parentApplicationName;
        private final short parentServiceType;

        public CacheKey(String host, String applicationName, short serviceType, String parentApplicationName, short parentServiceType) {
            if (host == null) {
                throw new NullPointerException("host must not be null");
            }
            if (applicationName == null) {
                throw new NullPointerException("bindApplicationName must not be null");
            }
            this.host = host;
            this.applicationName = applicationName;
            this.serviceType = serviceType;

            // may be null for below two parent values.
            this.parentApplicationName = parentApplicationName;
            this.parentServiceType = parentServiceType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CacheKey cacheKey = (CacheKey) o;

            if (parentServiceType != cacheKey.parentServiceType) return false;
            if (serviceType != cacheKey.serviceType) return false;
            if (!applicationName.equals(cacheKey.applicationName)) return false;
            if (!host.equals(cacheKey.host)) return false;
            if (parentApplicationName != null ? !parentApplicationName.equals(cacheKey.parentApplicationName) : cacheKey.parentApplicationName != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = host.hashCode();
            result = 31 * result + applicationName.hashCode();
            result = 31 * result + (int) serviceType;
            result = 31 * result + (parentApplicationName != null ? parentApplicationName.hashCode() : 0);
            result = 31 * result + (int) parentServiceType;
            return result;
        }
    }

}
