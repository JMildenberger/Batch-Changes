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
	Out-Rtl configuration */

Proc sql;
	Create table	work.OutRtl as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-Rtl.sas";   
quit;


/*	The Year Number is extracted from the variable YearID	*/

data work.IPS_SourceData;
	set work.OutRtl;
	YearNo=input(substr(YearID,5,1),1.);
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
	select			distinct IndustryID, CensusPeriodID, DataArrayID 
	from			work.IPS_SourceData
	where			DataSeriesID="XT32";
quit;

proc sql;
	create table 	work.IndYearsProdStruc as
	select			a.IndustryID, a.YearID, a.CensusPeriodID, a.YearNo, b.DataArrayID
	from			work.IndYears a
	left join	    work.IndYearsProd b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	order by		IndustryID, YearID, DataArrayID;
quit;


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


/*	Census Merchandise Line Sales | XT05=CenMLS */

proc sql;
	create table	work.MerchLineSales as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataseriesID="XT05";
quit;


/*	Census Product Lines | XT32=Sale## */

proc sql;
	create table 	work.ProdLines as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT32";
quit;


/*	ProdSum */
/*	This query sums all product values for each year in each industry */

proc sql;
	create table	work.ProdSum as
	select			IndustryID, "Prod Sum" as DataSeriesLabel, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT32"
	group by		IndustryID, DataseriesID, YearID, YearNo, CensusPeriodID;
quit;


/*	MLSUSedCenYears | ProdSum*/
/* This query calculates MLSUsed during census years by dividing ProdSum by 1000 */

proc sql;
	create table	work.MLSUsedCenYears as
	select			IndustryID, "MLSUsedCenYears" as DataSeriesLabel, YearID, YearNo, CensusPeriodID, 
					Value/1000 as Value
	from			work.ProdSum;
quit;


/*	Census All Establishment Sales | XT04=CenAES */

proc sql;
	create table	work.CenAES as
	select			IndustryID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT04";
quit;


/*	Product Deflators | XT06=Defl */

proc sql;
	create table	work.Defl as
	select			IndustryID, DataseriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT06";
quit;


/*	Rebase the Product Deflators */

proc sql;
	create table	work.RebasedDefl as
	select			a.IndustryID, "Rebased Defl" as DataSeriesLabel, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.Defl a
	inner join		work.Defl b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayID=b.DataArrayID) and (b.YearNo=1)
	order by 		a.IndustryID, a.DataSeriesID, a.DataArrayID, a.YearID ;			
quit;


/*	This query finds the ratio of the sum of product values to Census All Establishment Sales | Prodsum, CenAES | Prodsum*CenAES */

proc sql;
	create table	work.MLSRatioCenYears as
	select			a.IndustryID, "MLS Ratio Census Years" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, "00" as DataArrayID, a.Value/b.Value as Value
	from			work.MLSUsedCenYears a
	inner join		work.CenAES b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	Interpolate the MLS Ratio in census years to an annual basis */

%InterpolateIndustry (MLSRatioCenYears, IndYears);


/*	MLSUsed | T81=MLSUsed */
/*	This query calculates MLSUsed by multiplying the annual Census all establishment sales by */
/*	the used MLS coverage ratio */

proc sql;
	create table	work.MLSUsed as
	select			a.IndustryID, "T81" as DataSeriesID, a.CensusPeriodID, a.YearID, a.YearNo, "00" as DataArrayID, 
					a.Value*b.Value as Value
	from			work.AnnualMLSRatioCenYears a
	inner join		work.CenAES b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	Merchandise Line Sales Adjustment Ratio | T91=MLSRatio | CenAES,MLSUsed | CenAES/MLSUsed */
/*	This query calculates the MLS adjustment ratio by dividing Census All Establishment Sales by the previously calculated MLS used value */

proc sql;
	create table	work.MLSRatioCenYears as
	select			a.IndustryID, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value as Value
	from			work.CenAES a
	inner join		work.MLSUsedCenYears b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	Interpolate the MLS adjustment ratio from census years to an annual basis */

%InterpolateIndustry (MLSRatioCenYears, IndYears);


/*	MLSRatio | T91=MLSRatio */
/*	This query cleans up the annualized MLS adjustment ratio data */

proc sql;
	create table	work.MLSRatio as
	select			IndustryID, "T91" as DataSeriesID, CensusPeriodID, YearID, YearNo, DataArrayID, Value
	from			work.AnnualMLSRatioCenYears;
quit;


/*	Calculating AdjLineSales | ProdLines, MLSRatio | ProdLines*MLSRatio*/
/*	This query adjusts each product line sales value by the MLS Ratio*/

proc sql;
	create table 	work.AdjLineSales as
	select			a.IndustryID, "AdjLineSales" as DataSeriesLabel, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value*b.Value/1000 as Value
	from			work.ProdLines a
	inner join		work.MLSRatioCenYears b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	This query sums up the MLSRatio-adjusted product line values*/

proc sql;
	create table	work.TotalAdjLineSales as
	select			IndustryID, "TotalAdjLineSales" as DataSeriesLabel, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.AdjLineSales
	group by		IndustryID, YearID, YearNo, CensusPeriodID;
quit;


/*	SalesProportions | AdjLineSales, TotalAdjLineSales | AdjLineSales/TotalAdjLineSales*/
/*	This query finds the proportion of adjusted product sales to the sum total of adjusted product sales*/

proc sql;
	create table	work.SalesProportions as
	select			a.IndustryID, "SalesProportions" as DataSeriesLabel, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, a.Value/b.Value as Value
	from			work.AdjLineSales a
	inner join		work.TotalAdjLineSales b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	This query interpolates the sales proportion data */

%InterpolateProducts (SalesProportions, IndYearsProdStruc);


/*	AnnualSales | CenAES, AnnualSalesProportions | CenAES*AnnualSalesProportions*/
/*	This query multiplies each product's sales proportion by the Census All Establishment Sales*/

proc sql;
	create table 	work.AnnualSales as
	select			a.IndustryID, "Annual Sales" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, b.DataArrayID, a.Value*b.Value as Value
	from			work.CenAES a
	inner join		work.AnnualSalesProportions b
	on 				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/*	MLSSalesConstDollarProd | AnnualSales, RebasedDefl | AnnualSales/RebasedDefl */
/*	This query divides the annual product sales (AnnnualSales) by its matching rebased deflator (RebasedDefl) and multiplying by 100*/

proc sql;
	create table	work.MLSSalesConstDollarProd as
	select			a.IndustryID, "MLS Sales Const Dollar Prod" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, a.Value/b.Value*100 as Value
	from			work.AnnualSales a
	inner join 		work.RebasedDefl b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataArrayID=b.DataArrayID);
quit;


/*	MLSSalesConstDollar | MLSSalesConstDollarProd | sum(MLSSalesConstDollarProd) */
/*	This query sums up the annual constant product sales */

proc sql;
	create table	work.MLSSalesConstDollar as
	select			IndustryID, "MLS Sales Const Dollar" as DataSeries, CensusPeriodID, YearID, YearNo, 
					sum(Value) as Value
	from			work.MLSSalesConstDollarProd
	group by 		IndustryID, YearID, YearNo, CensusPeriodID;
quit;


/*	Calculating ConstDollarIdx | MLSSalesConstDollar | MLSSalesConstDollar[t]/MLSSalesConstDollar[base]*100 */

proc sql;
	create table	work.ConstDollarIdx as
	select			a.IndustryID, "ConstDollarIdx" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, 
					"00" as DataArrayID, a.Value/b.Value*100 as Value
	from			work.MLSSalesConstDollar a
	inner join		work.MLSSalesConstDollar b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);
quit;


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs */

proc sql;

	Create table  	work.Sub_MLSSalesConstDollarProd as 
    Select          IndustryID, CensusPeriodID, DataSeries, DataArrayID, YearID, YearNo,					 
					case when value = 0 then 0.001
						 else value
					end as value
    from 	     	work.MLSSalesConstDollarProd ;


/*	ConstDollarLnDiff | Sub_MLSSalesConstDollarProd | Sub_MLSSalesConstDollarProd[t]-Sub_MLSSalesConstDollarProd[t-1] */
/*	This query calculates the log change in the constant dollar production */

	create table	work.ConstDollarLnDiff as
	select			a.IndustryID, "ConstDollarLnDiff" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, log(a.Value)-log(b.Value) as Value
	from			work.Sub_MLSSalesConstDollarProd a
	left join		work.Sub_MLSSalesConstDollarProd b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);

quit;


/*	SalesProportionAvg | SalesPropotions | log(SalesProportions[t]) - log(SalesProportions[t-1]) */
/*	Calculating the 2-year moving averages in the sales proportion values */

proc sql;
	create table	work.SalesProportionAvg as
	select			a.IndustryID, "SalesProportionAvg" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, (b.Value+a.Value)/2 as Value
	from			work.AnnualSalesProportions a
	left join		work.AnnualSalesProportions b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);
quit;


/*	OutIndexShares | ConstDollarLnDiff, SalesProportionAvg | ConstDollarLnDiff*SalesProportionAvg */
/*	This query calculates the output index shares for each product line */

proc sql;
	create table	work.OutIndexShares as
	select			a.IndustryID, "OutIndexShares" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, a.Value*b.Value as Value
	from			work.ConstDollarLnDiff a
	inner join		work.SalesProportionAvg b
	on				(a.IndustryID=b.IndustryID) and (a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID);
quit;


/*	SumOutIdx | OutIndexShares | sum(OutIndexShares) */
/* 	This query sums the output shares across product lines for each industry */

proc sql;
	create table	work.SumOutIdx as
	select			IndustryID, "SumOutIdx" as DataSeriesLabel, CensusPeriodID, YearID, YearNo, sum(Value) as Value
	from			work.OutIndexShares
	group by		IndustryID, YearID, CensusPeriodID, YearNo;
quit;


/*	ExpSum | OutIndexShares | sum(SumOutIdx) */
/* 	This query sums the output shares across product lines for each industry */

proc sql;
	create table	work.ExpSum as
	select			IndustryID, "ExpSum" as DataSeriesLabel, CensusPeriodID, YearID, YearNo, exp(Value) as Value
	from			work.SumOutIdx;
quit;


/*	Calculating AnnOut (T37) via chain linking */

proc sql;
	Create table 	work.AnnOut as
	Select 			a.IndustryID, a.CensusPeriodID, "T37" as DataSeriesID, a.YearID, a.YearNo, "00" as DataArrayID,
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


/*	AnnVP */

proc sql;
	create table	work.AnnVP as
	select			IndustryID, "T36" as DataseriesID, CensusPeriodID, YearID, YearNo, DataArrayID, Value
	from			work.CenAES;
quit;


/*	ValShip */

proc sql;
	create table	work.ValShip as
	select			IndustryID, "T40" as DataseriesID, CensusPeriodID, YearID, YearNo, DataArrayID, Value
	from			work.CenAES;
quit;


/*	Calculating the current dollar index */

proc sql;
	create table	work.CurrentDollarIdx as
	select			a.IndustryID, "Current Dollar Index" as DataSeriesLabel, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value*100 as Value
	from			work.CenAES a
	inner join		work.CenAES b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);
quit;


/*	Output Adjustment Ratio	| T90=OutAdRat | OutputIndex,CurrentDollarIdx | OutputIndex*CurrentDollarIdx */

proc sql;
	create table	work.OutAdjRat as
	select			a.IndustryID, "T90" as DataSeriesID, a.YearID, a.YearNo, a.CensusPeriodID, a.DataArrayID, 
					a.Value/b.Value as Value
	from			work.AnnOut a
	inner join		work.ConstDollarIdx b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;


/* Merging calculated variables together along with source data variables */

proc sql;
	Create table 	work.OUTRtlFinalVars as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.MLSUsed union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdjRat union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.MLSRatio
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OUTRtlFinalVars union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.TradeSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
