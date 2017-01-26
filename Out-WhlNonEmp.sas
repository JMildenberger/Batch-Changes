libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for mining. */

data work.TradeSource;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.OutputSource as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.TradeSource
	where 			substr(DataSeriesID,1,2)="XT"
	order by 		IndustryID, DataSeriesID, YearID;
quit;


/*    This query isolates the configuration concordance stored in IPS to just Output */
Proc sql;
	Create table	work.ConfigDistinct as											
	Select 			IndustryID, IndustrySeriesID, CensusPeriodID, Program, Method	        
	from 			LPAll.ProgramMethodControlTable                                
	where 			IndustrySeriesID="Output";
quit;


/* 	This query uses the configuration concordance to filter only Industry/CensusPeriodIDs that use the 
	Out-WhlNonEmp configuration */

Proc sql;
	Create table	work.OutWhlNonEmp as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-WhlNonEmp.sas";   
quit;


/*	The Year Number is extracted from the variable YearID	*/

data work.IPS_SourceData;
	set work.OutWhlNonEmp;
	YearNo=input(substr(YearID,5,1),1.);
	if value = . then value=0;
run;


/*	IndYears creates the list of distinct industry data values*/

proc sql;
	create table 	work.IndYears as
	select			distinct IndustryID, YearID, CensusPeriodID, YearNo
	from			work.IPS_SourceData;
quit;


/*	IndYearsProdStruct creates the list of distinct product data values*/

proc sql;
	create table 	work.IndYearsProd as
	select			distinct IndustryID, YearID, YearNo, CensusPeriodID, DataArrayID 
	from			work.IPS_SourceData
	where			DataSeriesID="XT06";
quit;


/*	Interpolation Industry Macro */ 

%macro InterpolateIndustry (SourceData, StructureData);
Proc sql;

	Create table  	work.Diff&SourceData as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayID, (a.Value-b.Value)/5 as IncrementValue
    from 	     	&SourceData a
	inner join		&SourceData b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearNo=6) and (b.YearNo=1);

	Create table	work.Working&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, b.DataArrayID, b.Value, 
					case 	when c.IncrementValue is null then 0 
							else c.IncrementValue 
					end 	as IncrementValue
	from			&StructureData a 
	left join 		&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID)
	left join 		Diff&SourceData c
	on 				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) 
	order by 		a.IndustryID, b.DataArrayID, a.YearID;

	Create table	work.Annual&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, "Annual&SourceData" as Dataseries, a.YearID, a.YearNo, b.DataArrayID,
					(a.IncrementValue*(a.YearNo-1))+b.Value as Value
	from			work.Working&SourceData a
	inner join		work.Working&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1)
	order by 		a.IndustryID, b.DataArrayID, a.YearID;
quit;
%mend InterpolateIndustry;


/*	Interpolation Products Macro */ 

%macro InterpolateProducts (SourceData, StructureData);
Proc sql;

	Create table  	work.Diff&SourceData as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayID, (a.Value-b.Value)/5 as IncrementValue
    from 	     	&SourceData a
	inner join		&SourceData b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and 
					(a.YearNo=6) and (b.YearNo=1);

	Create table	work.Working&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, b.Value, 
					case 	when c.IncrementValue is null then 0 
							else c.IncrementValue 
					end 	as IncrementValue
	from			&StructureData a 
	left join 		&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataArrayID=b.DataArrayID) 
	left join 		Diff&SourceData c
	on 				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.DataArrayID=c.DataArrayID) 
	order by 		a.IndustryID, b.DataArrayID, a.YearID;

	Create table	work.Annual&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, "Annual&SourceData" as Dataseries, a.YearID, a.YearNo, b.DataArrayID,
					(a.IncrementValue*(a.YearNo-1))+b.Value as Value
	from			work.Working&SourceData a
	inner join		work.Working&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and (b.YearNo=1)
	order by 		a.IndustryID, b.DataArrayID, a.YearID;
quit;
%mend InterpolateProducts;

Proc sql;

/*	MSBO Sales | XT24=MSBSales */

	create table 	work.MSBSales as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT24";

/*	Merchant Wholesalers Deflator | XT25=MWDefl */

	create table 	work.MWDefl as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT25";

/*	Merchant Wholesalers | XT26=MWSales */

	create table 	work.MWSales as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT26";

/*	MSBO Percent Distribution of Sales to Other Wholesalers | XT28=PerMSB */

	create table 	work.PerMSB as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT28";

/*	Merchant Wholesalers Percent Distribution of Sales to Other Wholesalers | XT29=PerMW */

	create table 	work.PerMW as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT29";

/*	MSBO Line Item Sales | XT32=Sale## */

	create table 	work.MSBProductSales as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT32";

/*	Nonemployer Sales | XT50=MWNonEmp */

	create table 	work.MWNonEmp as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			IPS_SourceData
	where			DataSeriesID="XT50";

quit;


/*	Interpolate Intra-Industry Adjustment Ratios */

%InterpolateIndustry(PerMSB, IndYears);


/*	This query calculates the MSBO Intra-Industry Sales */

proc sql;
	create table	work.MSBIntraInd as
	select			a.IndustryID, "MSBIntraInd" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, a.Value*b.Value/100 as Value
	from			work.MSBSales a
	inner join		work.AnnualPerMSB b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	VPMSBO | T34=VPMSBO | MSBO Sales - MSBO Intra-Industry Sales */
/*	This query calculates the MSBO Value of Production */

proc sql;
	create table	work.VPMSBO as
	select			a.IndustryID, "T34" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, a.Value-b.Value as Value
	from			work.MSBSales a
	inner join		work.MSBIntraInd b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	Interpolate Intra-Industry Adjustment Ratios */

%InterpolateIndustry(PerMW, IndYears);


/*	This query sums merchant wholesalers sales and non-employer receipts */

proc sql;
	create table	work.MWTotalSales as
	select			a.IndustryID, "MW Total Sales" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value+b.Value as Value
	from			work.MWSales a
	inner join		work.MWNonEmp b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);


/*	This query calculates the MW intra-industry sales */

	create table	work.MWIntraInd as
	select			a.IndustryID, "MWIntraInd" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, a.Value*b.Value/100 as Value
	from			work.MWTotalSales a
	inner join		work.AnnualPerMW b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);


/*	VPMW | T35=VPMW | MW Sales - MW Intra-Industry Sales */
/*	This query calculates the MW Value of Production */

	create table	work.VPMW as
	select			a.IndustryID, "T35" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, "XT25" as DeflMatch, a.Value-b.Value as Value
	from			work.MWTotalSales a
	inner join		work.MWIntraInd b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);


/*	AnnVP | T36=AnnVP | VPMSBO + VPMW */
/*	This query calculates the Value of Production */

	create table	work.AnnVP as
	select			a.IndustryID, "T36" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, 
					case 	when  c.Method="NoManf" then a.Value
							when c.Method="Standard" then a.Value+b.Value
					end 	as 	Value 
	from			work.VPMW a
	left join		work.VPMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.ConfigDistinct c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);


/*	VSMSBO | T44=VSMSBO | MSBSales */
/*	This query sets MSB Value of Shipments equal to MSB Sales */

	create table	work.VSMSBO as
	select			IndustryID, "T44" as DataSeriesID, CensusPeriodID, YearID, YearNo, DataArrayID, Value
	from			work.MSBSales;


/*	VSMW | T45=VSMW | MWSales */
/*	This query sets MW Value of Shipments equal to MW Sales */

	create table	work.VSMW as
	select			IndustryID, "T45" as DataSeriesID, CensusPeriodID, YearID, YearNo, DataArrayID, Value
	from			work.MWSales;


/*	ValShip | T40=ValShip | VPMW + VPMSBO + MWNonEmp */
/*	This query calculates the Value of Shipments */

	create table	work.ValShip as
	select			a.IndustryID, "T40" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID,
					case 	when  d.Method="NoManf" then a.Value+c.Value
							when d.Method="Standard" then a.Value+b.Value+c.Value
					end 	as 	Value 
	from			work.VSMW a
	left join		work.VSMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.MWNonEmp c
	on				(a.IndustryID=c.IndustryID) and (a.YearID=c.YearID)
	left join		work.ConfigDistinct d
	on				(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID);


/*	IntraInd | T52=IntraInd | MSBIntraInd + MWIntraInd */
/*	This query sums up the Intra-Industry Value of Shipments */

	create table	work.IntraInd as
	select			a.IndustryID, "T52" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID,
					case 	when  c.Method="NoManf" then a.Value
							when c.Method="Standard" then a.Value+b.Value
					end 	as 	Value 
	from			work.MWIntraInd a
	left join		work.MSBIntraInd b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.ConfigDistinct c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);
quit;


/*	MSB Line Item Revenue */
/*	This query sums up MSBO line item revenue within each industry */

proc sql;
	create table	work.MSBLineItemRev as
	select			IndustryID, "MSBO Line Item Revenue" as DataSeries, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.MSBProductSales
	group by		IndustryID, YearID, YearNo, CensusPeriodID;


/*	MSBO Line Item Adjusted Ratio */
/*	This query calculates a ratio of MSBO value of production to the sum of MSBO line item revenue */

	create table	work.MSBLineItemAdjRatioCenYears as
	select			a.IndustryID, "MSBO Line Item Adjustment Ratio" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value as Value
	from			work.VPMSBO a
	inner join		work.MSBLineItemRev b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);


/*	Adjusted MSBO Line Item Sales */
/*	This query adjusts MSBO line item revenues */

	create table	work.MSBLineItemSalesAdj as
	select			a.IndustryID, "Adj MSBO Line Item Sales" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, b.DataArrayID, 
					a.Value*b.Value as Value
	from			work.MSBLineItemAdjRatioCenYears a
	inner join		work.MSBProductSales b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);


/*	Total MSBO Line Item Sales */
/*	This query sums up the adjusted MSBO line item revenue numbers for each industry */

	create table	work.TotalMSBLineItemSalesAdj as
	select			IndustryID, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.MSBLineItemSalesAdj
	group by 		IndustryID, YearID, YearNo, CensusPeriodID;


/*	MSBO Line Item Proportions */
/*	This query calculates the proportions of MSB adjusted line item sales in census years */

	create table	work.MSBLineItemProportions as
	select			a.IndustryID, "MSBLineItemProportions" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value as Value
	from 			work.MSBLineItemSalesAdj a
	inner join		work.TotalMSBLineItemSalesAdj b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);

quit;


/*	Interpolate MSBO Line Item Proportions */

%InterpolateProducts(MSBLineItemProportions, IndYearsProd);


/*	Industry Current Dollar Production */
/*	This query calculates MSB Line Current Dollar Production */

proc sql;
	create table	work.MSBLineCurrentDollarProd as
	select			a.IndustryID, b.DataSeriesID, "IndCurrentDollarProd" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, "XT06" as DeflMatch,
					a.Value*b.Value as Value
	from 			work.AnnualMSBLineItemProportions a
	inner join		work.VPMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);			


/*Merging production data together for Torqnvist process */

	Create table	work.AllCurrentProductionData as 
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			work.MSBLineCurrentDollarProd 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			work.VPMW;
quit;


/*	This query rebases MSB line item deflators */

proc sql;
	create table 	work.RebasedDefl as
	select 			a.IndustryID, a.DataSeriesID, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value*100 as Value
	from			work.IPS_SourceData a
	inner join		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and 
					(a.DataSeriesID=b.DataSeriesID) and (b.YearNo=1)
	where			a.DataSeriesID in ("XT06", "XT25");

	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllCurrentProductionData a	
	inner join		work.RebasedDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DeflMatch=b.DataseriesID)and (a.DataArrayID=b.DataArrayID) and(a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs */

proc sql;

	Create table  	work.Sub_ConstantDollarProduction as 
    Select          IndustryID, CensusPeriodID, DataseriesID, DataArrayID, YearID, YearNo,					 
					case when value = 0 then 0.001
						 else value
					end as value
    from 	     	work.ConstantDollarProduction ;

	Create table  	work.LogarithmicChange as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					log(a.value)-log(b.value) as value
    from 	     	work.Sub_ConstantDollarProduction a 
	left join 		work.Sub_ConstantDollarProduction b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID)and (a.DataArrayID=b.DataArrayID) and 
					(a.YearNo-1=b.YearNo);

quit;


/*	Calculating annual product shares of Current Dollar Production */

proc sql;
	Create table  	work.AnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					a.value/sum(a.value) as value
    from 	     	work.AllCurrentProductionData a 
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;


/*	Calculating average annual product shares of Current Dollar Production */

	Create table  	work.AverageAnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					(a.value+b.value)/2 as value
    from 	     	work.AnnualShares a 
	left join 		work.AnnualShares b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);


/*	Calculating exponent of sum of weighted product growth rates | Exp (Sum(LogarithmicChange*AverageAnnualShares))*/

	Create table  	work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
    from 	     	work.LogarithmicChange a
	inner join		work.AverageAnnualShares b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	group by		a.IndustryID, a.CensusPeriodID,  a.YearID, a.YearNo;


/*	Calculating AnnOut (T37) via chain linking*/

	Create table 	work.AnnOut as
	Select 			a.IndustryID, a.CensusPeriodID, "T37" as DataSeriesID, a.YearID, a.YearNo, 
					case 	when a.YearNo=1 then 100
							when a.YearNo=2 then b.value*100
							when a.YearNo=3 then b.value*c.value*100
							when a.YearNo=4 then b.value*c.value*d.value*100
							when a.YearNo=5 then b.value*c.value*d.value*e.value*100
							when a.YearNo=6 then b.value*c.value*d.value*e.value*f.value*100
					end 	as Value
	from 			work.ExpSum a 
	left join 		work.ExpSum b
	on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and b.YearNo=2 
	left join 		work.ExpSum c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and c.YearNo=3 
	left join 		work.ExpSum d
	on				(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID)and d.YearNo=4 
	left join 		work.ExpSum e
	on				(a.IndustryID=e.IndustryID) and (a.CensusPeriodID=e.CensusPeriodID)and e.YearNo=5 
	left join 		work.ExpSum f
	on				(a.IndustryID=f.IndustryID) and (a.CensusPeriodID=f.CensusPeriodID)and f.YearNo=6;
quit;


/*	VPMSBOIdx */
/*	This query calculates the index for VPMSBO */

proc sql;

	create table	work.VPMSBOIdx as
	select			a.IndustryID, "VPMSBOIdx" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from 			work.VPMSBO a
	inner join		work.VPMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);			


/*	Industry Constant Dollar Production Sum */

	create table	work.MSBLineConstDollarProdSum as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, "00" as DataArrayID, 
					sum(Value) as Value
	from			work.ConstantDollarProduction
	where			DataSeriesID="T34"
	group by		IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID;

	create table	work.MSBLineConstDollarProdSumIdx as
	select			a.IndustryID, "Constant Dollar Production Index" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, 
					a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.MSBLineConstDollarProdSum a
	Inner join		work.MSBLineConstDollarProdSum b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);


/*	DeflMSBO | T64=DeflMSBO | MSBSalesIdx / MSBLineConstDollarProdSumIdx * 100 */
/*	This query calculates the price deflator for Manufacturing Sales and Branch Offices */

	create table	work.DeflMSBO as
	select			a.IndustryID, "T64" as DataSeriesID, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.VPMSBOIdx a
	inner join		work.MSBLineConstDollarProdSumIdx b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);


/*	MW Sales Index */
/*	This query calculates the index for Merchant Wholesalers' Sales */

	create table	work.VPMWIdx as
	select			a.IndustryID, "VPMWIdx" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from 			work.VPMW a
	inner join		work.VPMW b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);			


	/*	Industry Constant Dollar Production Index */

	create table	work.VPMWConst as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, "00" as DataArrayID, 
					sum(Value) as Value
	from			work.ConstantDollarProduction
	where			DataSeriesID="T35"
	group by		IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID;

	create table	work.VPMWConstIdx as
	select			a.IndustryID, "Constant Dollar Production Index" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, 
					a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.VPMWConst a
	Inner join		work.VPMWConst b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);


/*	DeflMW | T65=DeflMW | MWSalesIdx / IndConstDollarProdIdx * 100 */
/*	This query calculates the price deflator for Manufacturing Wholesalers */

	create table	work.DeflMW as
	select			a.IndustryID, "T65" as DataSeriesID, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.VPMWIdx a
	inner join		work.VPMWConstIdx b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);


/*	This query sums the constant dollar VPMSBO and constant dollar VPMW into constant dollar Value of Production */

	create table	work.AnnVPConst as
	select			IndustryID, YearID, YearNo, CensusPeriodID, "00" as DataArrayID, 
					sum(Value) as Value
	from			work.ConstantDollarProduction
	group by		IndustryID, YearID, YearNo, CensusPeriodID;


/*	Industry Constant Dollar Production Index */

	create table	work.AnnVPConstIdx as
	select			a.IndustryID, "AnnVP Const Index" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value*100 as Value
	from			work.AnnVPConst a
	inner join		work.AnnVPConst b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);


/*	OutAdRat | T90=OutAdRat | AnnOut * IndConstDollarProdIdx */
/*	This query calculates the output adjustment ratio by comparing the output index to the constant dollar production index */

	create table	work.OutAdRat as
	select			a.IndustryID, "T90" as DataSeriesID, a.YearID, a.YearNo, a.CensusPeriodID, a.Value/b.Value as Value
	from			work.AnnOut a
	inner join		work.AnnVPConstIdx b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);


/*	CombinedPriceDefl | AnnVP / AnnVPConst */
/*	This query calculates the combined price deflator by dividing current dollar AnnVP by constant dollar AnnVP */

	create table	work.CombPriceDefl as
	select			a.IndustryID, "CombPriceDefl" as DataSeries, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.AnnVP a
	inner join		work.AnnVPConst b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

quit;



/**********************************************/
/*Intra-Sectoral Shipments Current $ (T53-T58)*/
/**********************************************/

/*Intra-Sectoral Shipments  | XT08=IntSect1 (Table_3_IntraSect_5Digit)| XT08 = T53
							  XT09=IntSect2 (Table_3_IntraSect_4Digit)| XT09 = T54
							  XT10=IntSect3 (Table_3_IntraSect_3Digit)| XT10 = T55
							  XT11=IntSect4 (Table_3_IntraSect_Sector)| XT11 = T58
							  XT12=IntSect5 (Table_3_IntraSect_Combo1)| XT12 = T56
							  XT13=IntSect6 (Table_3_IntraSect_Combo2)| XT13 = T57            */
Proc sql;
	Create table  	work.IntraSect5d as 
    Select          IndustryID, CensusPeriodID, "T53" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT08";
				
	Create table  	work.IntraSect4d as 
    Select          IndustryID, CensusPeriodID, "T54" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT09";

	Create table  	work.IntraSect3d as 
    Select          IndustryID, CensusPeriodID, "T55" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT10";

	Create table  	work.IntraSectSc as 
    Select          IndustryID, CensusPeriodID, "T58" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT11";

	Create table  	work.IntraSectC1 as 
    Select          IndustryID, CensusPeriodID, "T56" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT12";

	Create table  	work.IntraSectC2 as 
    Select          IndustryID, CensusPeriodID, "T57" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData 
    where	 		DataSeriesID="XT13";

	Create table 	work.IntraSect as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect5d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect4d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect3d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectSc union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectC1 union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectC2
	order by		IndustryID, DataSeriesID, YearID;
quit; 


/* Calculate Intra-Sectoral Shipments (Constant $) | IntraSect5d, CombPriceDefl |  (IntraSect5d / CombPriceDefl)*100
										 		     IntraSect4d, CombPriceDefl |  (IntraSect4d / CombPriceDefl)*100
											 	     IntraSect3d, CombPriceDefl |  (IntraSect3d / CombPriceDefl)*100
											 	     IntraSectSc, CombPriceDefl |  (IntraSectSc / CombPriceDefl)*100
											         IntraSectC1, CombPriceDefl |  (IntraSectC1 / CombPriceDefl)*100
												     IntraSectC2, CombPriceDefl |  (IntraSectC2 / CombPriceDefl)*100                        	*/
Proc sql;
	Create table  	work.IntraSect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect5d a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.IntraSect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect4d a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect3d a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSectSc a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectC1_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectC1_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSectC1 a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectC2_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectC2_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSectC2 a
	inner join		work.CombPriceDefl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*Calculate Sectoral Production (Constant $) | Sum(ConstDollarProd), IntraSect5d_cons |  Sum(ConstDollarProd) - IntraSect5d_cons
										       Sum(ConstDollarProd), IntraSect4d_cons |  Sum(ConstDollarProd) - IntraSect4d_cons
											   Sum(ConstDollarProd), IntraSect3d_cons |  Sum(ConstDollarProd) - IntraSect3d_cons
											   Sum(ConstDollarProd), IntraSectSc_cons |  Sum(ConstDollarProd) - IntraSectSc_cons
											   Sum(ConstDollarProd), IntraSectC1_cons |  Sum(ConstDollarProd) - IntraSectC1_cons
											   Sum(ConstDollarProd), IntraSectC2_cons |  Sum(ConstDollarProd) - IntraSectC2_cons 	*/
Proc sql;
	Create table  	work.Sect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSect5d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSect4d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSect3d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "SectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSectSc_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectC1_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "SectC1_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSectC1_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectC2_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "SectC2_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    work.AnnVPConst a
	inner join		work.IntraSectC2_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/* Calculate Sectoral Production Index  (Constant $) | Sect5d_cons | Calculate Index of Sect5d_cons
													   Sect4d_cons | Calculate Index of Sect4d_cons
										   			   Sect3d_cons | Calculate Index of Sect3d_cons
													   SectSc_cons | Calculate Index of SectSc_cons
													   SectC1_cons | Calculate Index of SectC1_cons
													   SectC2_cons | Calculate Index of SectC2_cons	                                        */
Proc sql;
	Create table  	work.Index_Sect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_Sect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sect5d_cons a
	inner join		work.Sect5d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and
					b.YearNo=1;
				
	Create table  	work.Index_Sect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_Sect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sect4d_cons a
	inner join		work.Sect4d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					b.YearNo=1;

	Create table  	work.Index_Sect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_Sect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sect3d_cons a
	inner join		work.Sect3d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					b.YearNo=1;

	Create table  	work.Index_SectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_SectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.SectSc_cons a
	inner join		work.SectSc_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					b.YearNo=1;

	Create table  	work.Index_SectC1_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_SectC1_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.SectC1_cons a
	inner join		work.SectC1_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and  
					b.YearNo=1;

	Create table  	work.Index_SectC2_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_SectC2_cons" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.SectC2_cons a
	inner join		work.SectC2_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and
					b.YearNo=1;
quit;


/*************************************/
/**Sectoral Output Index (T11-T16)****/
/*************************************/
	
/*Calculate Sectoral Output	Index (T11-T16)  |   Index_Sect5d_cons, OutAdRat (output weighting effect) | Index_Sect5d_cons * OutAdRat
											     Index_Sect4d_cons, OutAdRat (output weighting effect) | Index_Sect4d_cons * OutAdRat
											     Index_Sect3d_cons, OutAdRat (output weighting effect) | Index_Sect3d_cons * OutAdRat
									        	 Index_SectSc_cons, OutAdRat (output weighting effect) | Index_SectSc_cons * OutAdRat
												 Index_SectC1_cons, OutAdRat (output weighting effect) | Index_SectC1_cons * OutAdRat
												 Index_SectC2_cons, OutAdRat (output weighting effect) | Index_SectC2_cons * OutAdRat	                */
Proc sql;
	Create table  	work.OutIndex_Sect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T11" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect5d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.OutIndex_Sect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T12" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect4d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.OutIndex_Sect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T13" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect3d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.OutIndex_SectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T14" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_SectSc_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.OutIndex_SectC1_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T15" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_SectC1_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.OutIndex_SectC2_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "T16" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_SectC2_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table 	work.SectOut as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect5d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect4d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect3d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_SectSc_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_SectC1_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_SectC2_cons
	order by		IndustryID, DataSeriesID, YearID;
quit;


/*******************************************/
/*Sectoral Production (Current $) (T21-T26)*/
/*******************************************/

/*Calculate Sectoral Production (Current $) (T21-T26)  |  AnnVP, IntSect1 |  AnnVP - IntSect1
											              AnnVP, IntSect2 |  AnnVP - IntSect2
											              AnnVP, IntSect3 |  AnnVP - IntSect3
									        	          AnnVP, IntSect4 |  AnnVP - IntSect4
												          AnnVP, IntSect5 |  AnnVP - IntSect5
												          AnnVP, IntSect6 |  AnnVP - IntSect6	                */
Proc sql;
	Create table  	work.Sect5d as 
    Select          a.IndustryID, a.CensusPeriodID, "T21" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
    from 	     	work.AnnVP a
	inner join		work.IntraSect5d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4d as 
    Select          a.IndustryID, a.CensusPeriodID, "T22" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
    from 	     	work.AnnVP a
	inner join		work.IntraSect4d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3d as 
    Select          a.IndustryID, a.CensusPeriodID, "T23" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	work.AnnVP a
	inner join		work.IntraSect3d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectSc as 
    Select          a.IndustryID, a.CensusPeriodID, "T24" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	work.AnnVP a
	inner join		work.IntraSectSc b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectC1 as 
    Select          a.IndustryID, a.CensusPeriodID, "T25" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	work.AnnVP a
	inner join		work.IntraSectC1 b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectC2 as 
    Select          a.IndustryID, a.CensusPeriodID, "T26" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	work.AnnVP a
	inner join		work.IntraSectC2 b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table 	work.SectVal as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect5d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect4d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect3d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectSc union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectC1 union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectC2
	order by		IndustryID, DataSeriesID, YearID;
quit;


/* Merging calculated variables together along with source data variables */

proc sql;
	Create table 	work.OUTWhlNonEmpFinalVars as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraSect union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPMW union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VSMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VSMW union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraInd union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.DeflMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.DeflMW union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdRat
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OUTWhlNonEmpFinalVars union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.TradeSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
