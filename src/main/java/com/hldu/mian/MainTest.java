package com.hldu.mian;

import com.hldu.impl.OptionServiceImpl;

public class MainTest {

    public static void main(String[] args) {
        OptionServiceImpl optionService = new OptionServiceImpl();
        String tableName = "testTable";
//        optionService.createTable(tableName,null,null);
//        optionService.insertRow(tableName);
//        optionService.findRow(tableName,"Name","lisi6");
        optionService.updateRow(tableName);
//        optionService.dropTable(tableName);
    }
}
