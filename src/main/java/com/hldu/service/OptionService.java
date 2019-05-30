package com.hldu.service;

import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;

public interface OptionService {

    //创建表
    public void createTable(String name, Schema schema, CreateTableOptions builder);

    //删除表
    public void dropTable(String tableName);

    //插入数据
    public void insertRow(String tableName);

    //查找数据
    public void findRow(String tableName,String column,String value);

    //更新数据
    public void updateRow(String tableName);

    //删除数据
    public void deleteRow(String tableName);


}
