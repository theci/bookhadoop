package com.aaa.etl.pojo;

import java.io.Serializable;
import java.util.Date;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StoredColumnPojo implements Serializable {

	private Date date;
	
	private float value;
	
	private String id;
	
	private String title;
	
	private String state;
	
	private String frequency_short;
	
	private String units_short;
	
	private String seasonal_adjustment_short;
	
	// Apache Spark SQL에서 사용할 DataFrame의 Schema 객체를 생성
	private static StructType structType = DataTypes.createStructType(new StructField[] {
	
		DataTypes.createStructField("date", DataTypes.DateType, false),
		DataTypes.createStructField("value", DataTypes.FloatType, true),
		DataTypes.createStructField("id", DataTypes.StringType, false),
		DataTypes.createStructField("title", DataTypes.StringType, false),
		DataTypes.createStructField("state", DataTypes.StringType, false),
		DataTypes.createStructField("frequency_short", DataTypes.StringType, false),
		DataTypes.createStructField("units_short", DataTypes.StringType, false),
		DataTypes.createStructField("seasonal_adjustment_short", DataTypes.StringType, false)
	});
	
	// Apache Spark SQL에서 사용할 DataFrame의 Schema 객체를 반납
	public static StructType getStructType() {
		return structType;
	}
	
	public Object[] getAllValues() {
		return new Object[] { date, value, id, title, state, frequency_short, units_short, seasonal_adjustment_short};
	}
}
