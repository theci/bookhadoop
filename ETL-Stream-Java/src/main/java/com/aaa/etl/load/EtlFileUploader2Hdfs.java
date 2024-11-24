package com.aaa.etl.load;

import java.util.Arrays;
import java.util.List;

import com.aaa.etl.pojo.EtlColumnPojo;
import com.aaa.etl.processor.Fred2Hdfs;
import com.aaa.etl.processor.Fred2Hdfs.FREQUENCY;
import com.aaa.etl.util.US_STATES;

public class EtlFileUploader2Hdfs {
	
	private static List<String> titleEarningsList = Arrays.asList(
			"Average Hourly Earnings of All Employees: Construction in ",
			"Average Hourly Earnings of All Employees: Education and Health Services in ",
			"Average Hourly Earnings of All Employees: Financial Activities in ",
			"Average Hourly Earnings of All Employees: Goods Producing in ",
			"Average Hourly Earnings of All Employees: Leisure and Hospitality in ",
			"Average Hourly Earnings of All Employees: Manufacturing in ",
			"Average Hourly Earnings of All Employees: Private Service Providing in ",
			"Average Hourly Earnings of All Employees: Professional and Business Services in ",
			"Average Hourly Earnings of All Employees: Trade, Transportation, and Utilities in "
	);
	
	private static String titlePoverty = "Estimated Percent of People of All Ages in Poverty for ";
	private static String titleRealGDP = "Real Gross Domestic Product: All Industry Total in ";
	private static String titleUnemployee = "Unemployment Rate in ";
	private static String titleHouseholdIncome = "Real Median Household Income in ";
	private static String titleTaxExemption = "Total Tax Exemptions for ";
	private static String titleLaborForce = "Civilian Labor Force in ";

	public static String getFilename(String str) {
		int istart = str.indexOf(":") + 2;
		int iend = str.indexOf(" in");
		String title = str.substring(istart, iend).replace(" ", "_").replace(",", "");
		String filename = "earnings_" + title + ".csv";
		
		return filename;
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Fred2Hdfs fred = new Fred2Hdfs();
		
		System.out.println("======================== Unemployee Annual");
		
		fred.clearInputFiles("src/main/outputs/", "unemployee_annual.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlUnemployeeDataAnnual = fred.getEtlListData(FREQUENCY.YEAR, state, titleUnemployee);
			fred.writeCsv2Hdfs("unemployee_annual.csv", etlUnemployeeDataAnnual);
		
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "unemployee_annual.csv", etlUnemployeeDataAnnual);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "unemployee_annual.csv", etlUnemployeeDataAnnual);
			}
		}
		
		System.out.println("======================== Real Median Household Income");
		
		fred.clearInputFiles("src/main/outputs/", "household_income.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlhouseholdIncomData = fred.getEtlListData(FREQUENCY.YEAR, state, titleHouseholdIncome);
			fred.writeCsv2Hdfs("household_income.csv", etlhouseholdIncomData);
		
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "household_income.csv", etlhouseholdIncomData);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "household_income.csv",	etlhouseholdIncomData);
			}
		}
		
		System.out.println("======================== Total Tax Exemptions");
		
		fred.clearInputFiles("src/main/outputs/", "tax_exemption.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlTaxExempData = fred.getEtlListData(FREQUENCY.YEAR, state, titleTaxExemption);
			fred.writeCsv2Hdfs("tax_exemption.csv", etlTaxExempData);
		
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "tax_exemption.csv", etlTaxExempData);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "tax_exemption.csv", etlTaxExempData);
			}
		}
		
		System.out.println("======================== Civilian Labor Force");
		
		fred.clearInputFiles("src/main/outputs/", "civilian_force.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlCivilForceData = fred.getEtlListData(FREQUENCY.YEAR, state, titleLaborForce);
			fred.writeCsv2Hdfs("civilian_force.csv", etlCivilForceData);
			
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "civilian_force.csv", etlCivilForceData);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "civilian_force.csv", etlCivilForceData);
			}
		}
		
		System.out.println("======================== Poverty");
		
		fred.clearInputFiles("src/main/outputs/", "poverty.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlPovertyData = fred.getEtlListData(FREQUENCY.YEAR, state, titlePoverty);
			fred.writeCsv2Hdfs("poverty.csv", etlPovertyData);
			
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "poverty.csv", etlPovertyData);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "poverty.csv", etlPovertyData);
			}
		}
		
		System.out.println("======================== Real GDP");
		
		fred.clearInputFiles("src/main/outputs/", "real_gdp.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlRealGDPData = fred.getEtlListData(FREQUENCY.YEAR, state, titleRealGDP);
			fred.writeCsv2Hdfs("real_gdp.csv", etlRealGDPData);
		
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "real_gdp.csv", etlRealGDPData);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "real_gdp.csv", etlRealGDPData);
			}
		}
		
		System.out.println("======================== Unemployee Monthly");
		
		fred.clearInputFiles("src/main/outputs/", "unemployee_monthly.csv");
		
		for (US_STATES state : US_STATES.values()) {
			List<EtlColumnPojo> etlUnemployeeDataMonthly = fred.getEtlListData(FREQUENCY.MONTH, state, titleUnemployee);
			fred.writeCsv2Hdfs("unemployee_monthly.csv", etlUnemployeeDataMonthly);
		
			if (state.ordinal() == 0) {
				fred.writeCsv2Local(true, "src/main/outputs/", "unemployee_monthly.csv", etlUnemployeeDataMonthly);
			} else {
				fred.writeCsv2Local(false, "src/main/outputs/", "unemployee_monthly.csv", etlUnemployeeDataMonthly);
			}
		}
		
		System.out.println("======================== Earnings");
		
		for (int i = 0 ; i < titleEarningsList.size() ; i++) {
			fred.clearInputFiles("src/main/outputs/", getFilename(titleEarningsList.get(i)));
		}
		
		for (int i = 0 ; i < titleEarningsList.size() ; i++) {
			for (US_STATES state : US_STATES.values()) {
				List<EtlColumnPojo> etlEarningsData = fred.getEtlListData(FREQUENCY.MONTH, state, titleEarningsList.get(i));
				String filename = getFilename(titleEarningsList.get(i));
		
				fred.writeCsv2Hdfs(filename, etlEarningsData);
				
				if (state.ordinal() == 0) {
					fred.writeCsv2Local(true, "src/main/outputs/", filename, etlEarningsData);
				} else {
					fred.writeCsv2Local(false, "src/main/outputs/", filename, etlEarningsData);
				}
			}
		}
		
		fred.closeStream();
		
		System.out.println("======================== Done!!!");
	}
}
