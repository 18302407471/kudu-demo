package com.hldu.impl;

import com.hldu.service.OptionService;
import com.hldu.util.KuduUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.LinkedList;
import java.util.List;

public class OptionServiceImpl implements OptionService {

    private KuduClient kuduClient = KuduUtil.getConf();

    public void createTable(String tableName, Schema schema, CreateTableOptions builder) {
        try{
            // 设置表的schema
            List<ColumnSchema> columns = new LinkedList<ColumnSchema>();
            columns.add(new ColumnSchema.ColumnSchemaBuilder("CompanyId", Type.INT32).key(true).build());//true表示是主键
            columns.add(new ColumnSchema.ColumnSchemaBuilder("WorkId", Type.INT32).key(false).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("Name", Type.STRING).key(false).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("Gender", Type.STRING).key(false).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("Photo", Type.STRING).key(false).build());
            Schema schema1 = new Schema(columns);
            //创建表时提供的所有选项
            CreateTableOptions options = new CreateTableOptions();
            // 设置表的replica备份和分区规则
            List<String> parcols = new LinkedList<String>();
            parcols.add("CompanyId");

            //设置表的备份数
            options.setNumReplicas(1);
//            //设置range分区
            options.setRangePartitionColumns(parcols);
//            //设置hash分区和数量
            options.addHashPartitions(parcols, 2);

            kuduClient.createTable(tableName,schema1,options);
            kuduClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void dropTable(String tableName) {
        try {
            kuduClient.deleteTable(tableName);
            kuduClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void insertRow(String tableName) {
        try {
            KuduTable kuduTable = kuduClient.openTable(tableName);
            KuduSession session = kuduClient.newSession();
            // 采取Flush方式 手动刷新
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(3000);
            for (int i = 1; i < 10; i++) {
                Insert insert = kuduTable.newInsert();
                // 设置字段内容
                insert.getRow().addInt("CompanyId", i);
                insert.getRow().addInt("WorkId", i);
                insert.getRow().addString("Name", "lisi" + i);
                insert.getRow().addString("Gender", "male");
                insert.getRow().addString("Photo", "person" + i);
                session.flush();
                session.apply(insert);
            }
            session.close();
            kuduClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void findRow(String tableName,String column,String value) {
        try{
            KuduTable table = kuduClient.openTable(tableName);
            KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(table);
            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn(column),KuduPredicate.ComparisonOp.EQUAL, value);
            builder.addPredicate(predicate);
            // 开始扫描
            KuduScanner scaner = builder.build();
            while (scaner.hasMoreRows()) {
                RowResultIterator iterator = scaner.nextRows();
                while (iterator.hasNext()) {
                    RowResult result = iterator.next();
                    /**
                     * 输出行
                     */
                    System.out.print("CompanyId:" + result.getInt("CompanyId") +"   ");
                    System.out.print("Name:" + result.getString("Name") +"  ");
                    System.out.print("Gender:" + result.getString("Gender")+"    ");
                    System.out.print("WorkId:" + result.getInt("WorkId") +"  ");
                    System.out.println("Photo:" + result.getString("Photo")+"    ");
                }
            }
            scaner.close();
            kuduClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void updateRow(String tableName) {
        KuduSession session = kuduClient.newSession();
        try {
            KuduTable table = kuduClient.openTable(tableName);
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            //更新数据
            Update update = table.newUpdate();
            PartialRow row1 = update.getRow();
            row1.addInt("CompanyId",1);
            row1.addString("Name","di");
            OperationResponse response = session.apply(update);
            System.out.println(response.getRowError());
        } catch (KuduException e) {
            e.printStackTrace();
        }finally {
            try {
                session.close();
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteRow(String tableName) {
        try{
            KuduTable table = kuduClient.openTable(tableName);
            // 创建写session,kudu必须通过session写入
            KuduSession session = kuduClient.newSession();
            final Delete delete = table.newDelete();
            //TODO 注意：行删除和更新操作必须指定要更改的行的完整主键;
            delete.getRow().addInt("CompanyId" , 5);
            session.flush();
            session.apply(delete);
            session.close();
            kuduClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
