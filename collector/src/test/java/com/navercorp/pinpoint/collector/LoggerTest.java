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

package com.navercorp.pinpoint.collector;

import com.navercorp.pinpoint.collector.util.EsIndexs;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author emeroad
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations="classpath:applicationContext-collector.xml")
public class LoggerTest {

    //@Autowired
    //EsIndexs esIndexs;
    @Test
    public void testValue(){
       // System.out.println(esIndexs.getListDay());
       // System.out.println(esIndexs.getListMonth());
    }


    @Test
    public void test(){
        String sr = EsIndexs.getIndex(EsIndexs.APPLICATION_MAP_STATISTICS_CALLER_VER2);
        System.out.println(sr);
    }


    @Test
    public void log() {
        Logger test = LoggerFactory.getLogger(LoggerTest.class);
        test.info("info");
        test.debug("debug");
    }
}
