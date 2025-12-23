package org.yhm.spark2.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.yhm.spark2.service.SparkService;

@RestController
@RequestMapping("/spark")
public class sparkController {

    @Autowired
    SparkService sparkService;

    // 俩个请求参数：2025-06-26 00:00:00 2025-06-26 00:00:10
    @RequestMapping("/query/{start}/{end}")
    public Object queryData(@PathVariable("start") String start, @PathVariable("end") String end) {
        return sparkService.queryData(start, end);
    }
}
