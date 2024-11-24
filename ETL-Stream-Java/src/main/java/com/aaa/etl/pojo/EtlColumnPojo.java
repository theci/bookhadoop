package com.aaa.etl.pojo;

import java.time.LocalDate;

import com.aaa.etl.util.CustomFloatDeserializer;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@JsonPropertyOrder({"realtime_start", "realtime_end", "date", "value", "id", "title",
	"state", "frequency_short", "units_short", "seasonal_adjustment_short"})
public class EtlColumnPojo {

	@JsonProperty("realtime_start")
	@JsonFormat(pattern="yyyy-MM-dd")
	@JsonDeserialize(using = LocalDateDeserializer.class)
	private LocalDate realtime_start;

	@JsonProperty("realtime_end")
	@JsonFormat(pattern="yyyy-MM-dd")
	@JsonDeserialize(using = LocalDateDeserializer.class)
	private LocalDate realtime_end;

	@JsonProperty("date")
	@JsonFormat(pattern="yyyy-MM-dd")
	@JsonDeserialize(using = LocalDateDeserializer.class)
	private LocalDate date;

	@JsonDeserialize(using = CustomFloatDeserializer.class) // "." 값에 대한 역직렬화
	@JsonProperty("value")
	private Float value;

	@JsonProperty("id")
	private String id;

	@JsonProperty("title")
	private String title;
	
	@JsonProperty("state")
	private String state;
	
	@JsonProperty("frequency_short")
	private String frequency_short;
	
	@JsonProperty("units_short")
	private String units_short;
	
	@JsonProperty("seasonal_adjustment_short")
	private String seasonal_adjustment_short;
	
	@Override
	public String toString() {
		return "EtlColumnPojo [realtime_start=" + realtime_start
				+ ", realtime_end=" + realtime_end
				+ ", date=" + date + ", value=" + value + ", id=" + id
				+ ", title=" + title + ", state=" + state
				+ ", frequency_short=" + frequency_short + ", units_short=" + units_short
				+ ", seasonal_adjustment_short=" + seasonal_adjustment_short + "]";
	}
	
	@JsonIgnore
	public String getValues() {
		String values = realtime_start + "," + realtime_end + "," + date + ","
				+ value + "," + id + "," + title + "," + state + "," + frequency_short
				+ "," + units_short + "," + seasonal_adjustment_short;
	
		return values;
	}
	
	@JsonIgnore
	public String getColumns() {
		String columns = "realtime_start" + "," + "realtime_end" + "," + "date" + ","
				+ "value" + "," + "id" + "," + "title" + "," + "state" + "," + "frequency_short"
				+ "," + "units_short" + "," + "seasonal_adjustment_short";
	
		return columns;
	}
}