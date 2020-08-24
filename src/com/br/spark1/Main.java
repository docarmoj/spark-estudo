package com.br.spark1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Main {
	
	public static void main(String[] args) {
		
		
		SparkSession spark = SparkSession
				.builder()
				.appName("SuperTesteYoutube")
				.getOrCreate();
		
		
		StructType schema = new StructType()
                .add("InvoiceNo", "string")
                .add("StockCode", "string")
                .add("Description", "string")
                .add("Quantity", "int")
                .add("InvoiceDate", "Date")
                .add("UnitPrice", "long")
                .add("CustomerID", "string")
                .add("Country", "string"); 
         
           Dataset<Row> df = spark.read()
                    .option("mode", "DROPMALFORMED")
                    .schema(schema)
                    .option("dateFormat", "dd/MM/yyyy")
				.csv("file:///d:/Downloads/data.csv");
		
        System.out.println( "Total registros: "+df.count() );
        
        df.createOrReplaceTempView("pedidos");
        Dataset<Row> sqlResult = spark.sql("SELECT InvoiceNo, InvoiceDate, CustomerID, SUM( UnitPrice*Quantity ) FROM pedidos GROUP BY InvoiceNo,InvoiceDate,CustomerID ORDER BY 4 DESC LIMIT 10");

        sqlResult.show();
		
		sqlResult.write().option("header", "true").csv("file:///d:/Downloads/saida");
	}		
	
}
