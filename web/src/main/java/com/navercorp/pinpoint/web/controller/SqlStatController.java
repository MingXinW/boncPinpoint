package com.navercorp.pinpoint.web.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.navercorp.pinpoint.web.service.SpanService;


/**
 * 
 * ClassName: SqlStatController <br/> 
 * Function: SQL统计. <br/> 
 * date: 2017年1月23日 下午3:59:55 <br/> 
 * 
 * @author jacob 
 * @version  
 * @since JDK 1.8
 */
@Controller
@RequestMapping("/transaction")
public class SqlStatController {
	
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SpanService spanService;

}
