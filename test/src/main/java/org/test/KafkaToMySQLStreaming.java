package org.test;

import cn.hutool.core.date.DateTime;
import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class KafkaToMySQLStreaming {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // 设置 Spark 配置
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToMySQL").setMaster("local");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Properties jdbcProperties = new Properties();
        jdbcProperties.setProperty("url", "jdbc:mysql://localhost:3306/test");
        jdbcProperties.setProperty("dbtable", "kafka_test");
        jdbcProperties.setProperty("user", "root");
        jdbcProperties.setProperty("password", "123456");

        Dataset<Row> kafkaStreamDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka.orb.local:9092")
                .option("subscribe", "mysql")
                .option("group.id", DateTime.now().toString())
                .option("startingOffsets", "latest")
                .load();


        String arg = args[0];
        Map map = JSON.parseObject(arg, Map.class);
        List<String> list = new ArrayList<>();

        StructType schema = generateSchema(map);
        // 将 JSON 字符串转换为 DataFrame，并修改字段名
        Dataset<Row> jsonDataFrame = kafkaStreamDF.selectExpr("CAST(value AS STRING)")
                .select(functions.from_json(functions.col("value"), schema)
                        .as("data"));

        Dataset<Row> flattenedDataFrame = recursiveFlatten(jsonDataFrame, "data", list);
        flattenedDataFrame = flattenedDataFrame.selectExpr(list.toArray(new String[0]));



        String[] cols = flattenedDataFrame.columns();
        StringBuilder sqlStr = new StringBuilder("INSERT INTO ")
                .append(jdbcProperties.get("dbtable"))
                .append("(")
                .append(String.join(", ", cols))
                .append(") VALUES(")
                .append(String.join(", ", Collections.nCopies(cols.length, "?")))
                .append(") ON DUPLICATE KEY UPDATE ")
                .append(String.join(", ", Arrays.stream(cols).map(field -> field + " = VALUES(" + field + ")").toArray(String[]::new)));
        String result_sql = sqlStr.toString();


        // 将数据写入 MySQL 表
        StreamingQuery query = flattenedDataFrame.writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.foreachPartition(iter -> {
                        HikariDataSource dataSource = DataSourceSingleton.getDataSourceInstance(jdbcProperties);
                        int count = 0;
                        try (Connection conn = dataSource.getConnection();
                             PreparedStatement ps = conn.prepareStatement(result_sql)){
                            conn.setAutoCommit(false);
                            while (iter.hasNext()) {
                                Row row = iter.next();
                                StructField[] fields = row.schema().fields();
                                for (int i = 0; i < fields.length; i++) {
                                    ps.setObject(i + 1, row.getAs(i));
                                }
                                ps.addBatch();
                                count += 1;
                                if (count % 10000 == 0) {
                                    ps.executeBatch();
                                }
                            }
                            ps.executeBatch();
                            conn.commit();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                })
                .start();

        query.awaitTermination();
    }


    public static StructType generateSchema(Map<String, Object> jsonMap) {
        StructType schema = new StructType();

        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {

            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            if (fieldValue instanceof Map) {
                // 如果字段值是 Map 类型，说明是嵌套结构，递归处理
                StructType nestedSchema = generateSchema((Map<String, Object>) fieldValue);
                schema = schema.add(fieldName, nestedSchema);
            } else if (fieldValue instanceof Iterable) {
                // 如果字段值是 Iterable 类型，说明是数组，递归处理数组元素类型
                DataType elementType = getArrayType((Iterable<?>) fieldValue);
                schema = schema.add(fieldName, ArrayType.apply(elementType));
            } else {
                // 否则，直接使用字段值的类型
                DataType fieldType = getSparkType(fieldValue);
                if (String.valueOf(entry.getValue()).startsWith("${")) {
                    //增加标识，用于后续dataset查询
                    schema = schema.add(fieldName, fieldType, true,"target");
                } else {
                    schema = schema.add(fieldName, fieldType);
                }
            }
        }

        return schema;
    }

    private static DataType getArrayType(Iterable<?> iterable) {
        // 获取数组元素类型
        DataType elementType = DataTypes.NullType;

        for (Object item : iterable) {
            if (item instanceof Map) {
                // 如果数组元素是 Map 类型，递归处理
                StructType nestedSchema = generateSchema((Map<String, Object>) item);
                elementType = nestedSchema;
                break;
            } else {
                // 否则，直接使用数组元素的类型
                DataType itemType = getSparkType(item);
                elementType = itemType;
                break;
            }
        }

        return elementType;
    }

    private static DataType getSparkType(Object value) {
        if (value instanceof Integer) {
            return DataTypes.IntegerType;
        } else if (value instanceof Double) {
            return DataTypes.DoubleType;
        } else if (value instanceof String) {
            return DataTypes.StringType;
        } else {
            return DataTypes.NullType;
        }
    }

    private static Dataset<Row> recursiveFlatten(Dataset<Row> inputDF, String columnName, List<String> list) {

        StructType schema = inputDF.select(columnName).schema();
        columnName = columnName.replace(".*", "");
        for (StructField field : schema.fields()) {
            if (field.dataType() instanceof ArrayType) {
                // 使用 explode_outer 对数组进行展开
                inputDF = inputDF.withColumn("exploded_" + field.name(), functions.explode_outer(functions.col(columnName + "." + field.name())));
                inputDF.printSchema();

                ArrayType arrayType = (ArrayType) field.dataType();
                if (arrayType.elementType() instanceof StructType) {
                    // 递归展开嵌套结构
                    inputDF = recursiveFlatten(inputDF, "exploded_" + field.name() + ".*",list);
                }
            } else if (field.dataType() instanceof StructType) {
                inputDF = recursiveFlatten(inputDF, field.name() + ".*",list);
            } else {
                if (!field.getComment().isEmpty()){
                    list.add(columnName + "." + field.name() + " AS " + field.name().replace("_target",""));
                }
            }
        }

        return inputDF;
    }

}
