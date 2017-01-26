libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for mining. */

data work.ServiceSource;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.OutputSource as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.ServiceSource
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
	Out-Service configuration */

Proc sql;
	Create table	work.OutService as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-Service.sas";   
quit;


/* To mimick what dataseries will be in the database once the new Inv program methods are implemented, this query will eliminate Inventory (XT07 and XT51) and 
Employer Firm Revenue (XT36) from industries that do not use the Inv methods */

Proc sql;
	Create table	work.ServiceNew as
	Select			*
	from			work.OutService
	where			Not(Method in ("Annual", "EndWt") and DataSeriesID in ("XT07", "XT51", "XT36"));
quit;

/*	The Year Number is extracted from the variable YearID	*/
data work.IPS_SourceData;
	set work.ServiceNew;
	YearNo=input(substr(YearID,5,1),1.);
	if Value = . then Value = 0;
run;


/*************
*InvChg (T50)*
**************/

/*	Unadjusted Change in Inventories | XT07 = InvEOYEF, XT51 = InvBOYEF |	year 01(XT07-XT51), year 02-06 (XT07 cur yr - XT07 prev yr) */
Proc sql;
	Create table	work.Unadj_InvChgY01 as 
	Select			a.IndustryID, a.CensusPeriodID, "Unadj_InvChg_Y01" as Dataseries, a.YearID, a.YearNo, (a.Value-b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearNo=1) and (b.YearNo=1)
					and (a.DataSeriesID="XT07") and (b.DataSeriesID="XT51");

	Create table	work.Unadj_InvChgY02_Y06 as 
	Select			a.IndustryID, a.CensusPeriodID, "Unadj_InvChg_Y02Y06" as Dataseries, a.YearID, a.YearNo,(a.Value-b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearNo>1)
					and (a.YearNo-1 = b.YearNo) and (a.DataSeriesID="XT07") and (b.DataSeriesID="XT07");

	Create table	work.Unadj_InvChg as 
	Select			IndustryID, CensusPeriodID, "Unadj_InvChg_Y01Y06" as Dataseries, YearID, YearNo, Value  from work.Unadj_InvChgY01 union all
	Select			IndustryID, CensusPeriodID, "Unadj_InvChg_Y01Y06" as Dataseries, YearID, YearNo, Value  from work.Unadj_InvChgY02_Y06
	order by		IndustryID, CensusPeriodID, YearID, YearNo;
quit;

	
/*	Inventory Adjustment Ratio | XT37=TotRevId, XT36=TotRevEF |	(XT37/XT36) */
Proc sql;
	Create table	work.InventoryAdjustmentRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "InvAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and(a.DataSeriesID="XT37") and 
					(b.DataSeriesID="XT36");
quit;


/*	Change in Inventories | Unadj_InvChg, InventoryAdjustmentRatio | (Unadj_InvChg * InventoryAdjustmentRatio) */
Proc sql;
	Create table	work.InvChg as 
	Select			a.IndustryID, a.CensusPeriodID, "T50" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.Unadj_InvChg a 
	inner join		work.InventoryAdjustmentRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/**************
*ValShip (T40)*
***************/

/*	Value of Shipments | XT37=TotRevId | (XT37) */
proc sql;
	Create table	work.ValShip as
	Select 			IndustryID, CensusPeriodID, YearID, YearNo, "T40" as DataSeriesID, Value 
	from 			work.IPS_SourceData
	where			DataSeriesID = "XT37";
quit;


/***************
*IntraInd (T52)*
****************/

/*	Intra Industry Shipments | XT41=VsIntra | (XT41) */
proc sql;
	Create table	work.IntraInd as
	Select 			IndustryID, CensusPeriodID, YearID, YearNo, "T52" as DataSeriesID, Value 
	from 			work.IPS_SourceData
	where			DataSeriesID = "XT41";
quit;

/************
*AnnVP (T36)*
*************/

/*  Annual Value of Production | XT37=TotRevId, InvChg, XT41=VSIntra | (TotRevID + InvChg - VSIntra)  */
Proc sql;
	Create table	work.AnnVP as 
	Select			a.IndustryID, a.CensusPeriodID, "T36" as DataseriesID, a.YearID, a.YearNo, 
				 	case 	when  d.Method in ("Annual", "EndWt") then a.Value-c.Value
							when d.Method in ("AnnualInv", "EndWtInv") then a.Value+b.Value-c.Value
					end as Value
	from 			work.ValShip a 
	left join 		work.InvChg b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo)
	left join 		work.IntraInd c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and 
					(a.YearID=c.YearID) and (a.YearNo=c.YearNo)
	left join		work.ConfigDistinct d
	on				(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID);
quit;


/*************
*AnnOut (T37)*
**************/

/*  Sources of Revenue Adjustment Ratio | AnnVP, XT32=Sale | (AnnVP / [Sum(XT32))  */
Proc sql;
	Create table	work.SourcesOfRevenueAdjustmentRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "SrcOfRevenueAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.AnnVP a
	inner join  	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID, 
								sum(Value) as Value 
					from 		work.IPS_SourceData
					group by 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and(b.DataSeriesID="XT32") ;
quit;

Proc sql;
	Create table	work.CurrentDollarProductionAnnWt as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayId, "CurrentDollarProduction" as Dataseries, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.SourcesOfRevenueAdjustmentRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and (a.DataSeriesID = "XT32") and (a.Method in ("Annual", "AnnualInv"))
	order by		IndustryID, CensusPeriodID, DataArrayID, YearID, YearNo;
quit;



/* Adjusted Sources of Revenue | XT32 = Sale, SourcesOfRevenueAdjustmentRatio | XT32 * SourcesOfRevenueAdjustmentRatio */
Proc sql;
	Create table	work.AdjSourcesRevenueEndWt as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayId, "AdjSourcesOfRevenue" as Dataseries, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.SourcesOfRevenueAdjustmentRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and (a.DataSeriesID = "XT32") and (a.Method in ("EndWt", "EndWtInv"))
	order by		IndustryID, CensusPeriodID, DataArrayID, YearID, YearNo;
quit;

proc sql;
	create table 	work.ProdStructureEndWt as
	select			distinct IndustryID, YearID, CensusPeriodID, YearNo, DataArrayID
	from			work.IPS_SourceData
	where			DataSeriesID="XT06" and Method in ("EndWt", "EndWtInv");
quit;

/* Sales Proportions | AdjSourcesRevenue, Sum(AdjSourcesRevenue) | interpolation of [AdjSourcesRevenue / sum(AdjSourcesRevenue)] */
Proc sql;
	Create table	work.SalesProportionsEndWt as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayId, "SalesProportions" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.AdjSourcesRevenueEndWt a 
	inner join  	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeries, 
								sum(Value) as Value 
					from 		work.AdjSourcesRevenueEndWt
					group by 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeries) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/*Interpolate Sales Proporation Data*/
Proc sql;
	Create table  	work.DiffSalesProportionsEndWt as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayID, (a.Value-b.Value)/5 as IncrementValue
    from 	     	work.SalesProportionsEndWt a
	inner join		work.SalesProportionsEndWt b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) and
					(a.YearNo=6) and (b.YearNo=1);

	Create table	work.WorkingSalesProportionsEndWt as
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayID, a.YearID, a.YearNo, b.Value, 
					case 	when c.IncrementValue is null then 0 
							else c.IncrementValue 
					end 	as IncrementValue
	from			work.ProdStructureEndWt a 
	left join 		work.SalesProportionsEndWt b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) 
					and (a.YearID=b.YearID) 
	left join 		DiffSalesProportionsEndWt c
	on 				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.DataArrayID=c.DataArrayID);

	Create table	work.AnnualSalesProportionsEndWt as
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayID, "AnnualSalesProportions" as Dataseries, a.YearID, a.YearNo,
					(a.IncrementValue*(a.YearNo-1))+b.Value as Value
	from			work.WorkingSalesProportionsEndWt a
	inner join		work.WorkingSalesProportionsEndWt b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayID=b.DataArrayID) 
					and (b.YearNo=1);
quit;


/* Current Dollar Value of Production | AnnVP,  AnnualSalesProportions | AnnVp * AnnualSalesProportions  */
Proc sql;
	Create table  	work.CurrentDollarProductionEndWt as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayID, a.YearId, a.YearNo, (a.Value*b.Value) as Value
    from 	     	work.AnnualSalesProportionsEndWt a
	inner join		work.AnnVp b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearId=b.YearId) and (a.YearNo=b.YearNo);
quit;

Proc sql;
	Create table  	work.CurrentDollarProduction as 
    Select          IndustryID, CensusPeriodID, DataArrayID, YearId, YearNo, Value from work.CurrentDollarProductionAnnWt union all
    Select          IndustryID, CensusPeriodID, DataArrayID, YearId, YearNo, Value from work.CurrentDollarProductionEndWt;
quit;


/*Index of rebased deflators | XT06 = Defl | (Defl t+1 / Defl t *100)   */
proc sql;
	Create table  	work.RebasedDeflators as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "Rebased Deflators" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.IPS_SourceData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayId=b.DataArrayID) 
					and (a.DataSeriesID = "XT06") and (b.DataSeriesID = "XT06") and (b.YearNo=1)
	order by		IndustryID, CensusPeriodID, DataArrayID, YearID, YearNo;
quit;


/* Constant Dollar Value of Production | CurrentDollarProduction, RebasedDeflators | (CurrentDollarProduction / RebasedDeflators *100)   */
proc sql;
	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "ConstantDollarProduction" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.CurrentDollarProduction a
	inner join		work.RebasedDeflators b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataArrayId=b.DataArrayID) 
					and (a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs		*/
proc sql;
	Create table  	work.Sub_ConstantDollarProduction as 
    Select          IndustryID, CensusPeriodID, Dataseries, DataArrayID, YearID, YearNo,					 
					case when value = 0 then 0.001
						 else value
					end as value
    from 	     	work.ConstantDollarProduction ;

/*	Calculating Logarithmic Change in ConstantDollarProduction */
	Create table  	work.LogarithmicChange as 
    Select          a.IndustryID, a.CensusPeriodID, "LogChg ConstantDollarProduction" as Dataseries, a.DataArrayID, a.YearID, a.YearNo, 
					log(a.value)-log(b.value) as value
 	from 	     	work.Sub_ConstantDollarProduction a 
	left join 		work.Sub_ConstantDollarProduction b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.Dataseries=b.Dataseries)and (a.DataArrayID=b.DataArrayID) and 
					(a.YearNo-1=b.YearNo);
quit;


/*	Calculating Annual Product Shares of Current Dollar Production */
proc sql;
	Create table  	work.AnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, "Annual Shares" as Dataseries, a.DataArrayID, a.YearID, a.YearNo, 
					a.value/sum(a.value) as value
    from 	     	work.CurrentDollarProduction a 
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;


/*	Calculating Average Annual Product Shares of Current Dollar Production */
proc sql;
	Create table  	work.AverageAnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, "Average Annual Shares" as Dataseries, a.DataArrayID, a.YearID, a.YearNo, 
					(a.value+b.value)/2 as value
    from 	     	work.AnnualShares a 
	left join 		work.AnnualShares b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);
quit;

/*	Calculating exponent of sum of unweighted quantity growth rates | Exp (Sum(UnweightedQtyChange*AverageAnnualShares))*/
proc sql;
	Create table  	work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, "ExpSum of unweighted quantity growth rates" as Dataseries, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
    from 	     	work.LogarithmicChange a
	inner join		work.AverageAnnualShares b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	group by		a.IndustryID, a.CensusPeriodID,  a.YearID, a.YearNo;
quit;


/*	Calculating AnnOut (T37) via chain linking*/
proc sql;
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


/**********************************************/
/*Intra-Sectoral Shipments Current $ (T53-T58)*/
/**********************************************/

/*Intra-Sectoral Shipments  | XT08=IntSect1 (Table_3_IntraSect_5Digit)| XT08 = T53
							  XT09=IntSect2 (Table_3_IntraSect_4Digit)| XT09 = T54
							  XT10=IntSect3 (Table_3_IntraSect_3Digit)| XT10 = T55
							  XT11=IntSect4 (Table_3_IntraSect_Sector)| XT11 = T58          */
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

	Create table 	work.IntraSect as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect5d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect4d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect3d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectSc
	order by		IndustryID, DataSeriesID, YearID;
quit; 


/*****************/
/***OutAdRat(T90)*/
/*****************/

/*Calculate Combined Price Deflator | AnnVp, Sum(ConstantDollarProduction) | AnnVp / Sum(ConstantDollarProduction)*100    */
proc sql;
	Create table  	work.CombPriceDfl as 
    Select          a.IndustryID, a.CensusPeriodID, "CombPriceDfl" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	 AnnVp a
	inner join		(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/* Calculate Intra-Sectoral Shipments (Constant $) | IntraSect5d, CombPriceDfl |  (IntraSect5d / CombPriceDfl)*100
										 		     IntraSect4d, CombPriceDfl |  (IntraSect4d / CombPriceDfl)*100
											 	     IntraSect3d, CombPriceDfl |  (IntraSect3d / CombPriceDfl)*100
											 	     IntraSectSc, CombPriceDfl |  (IntraSectSc / CombPriceDfl)*100                   	*/
Proc sql;
	Create table  	work.IntraSect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect5d a
	inner join		work.CombPriceDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.IntraSect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect4d a
	inner join		work.CombPriceDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSect3d a
	inner join		work.CombPriceDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value / b.Value)*100 as Value
    from 	     	work.IntraSectSc a
	inner join		work.CombPriceDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*Calculate Sectoral Production (Constant $) | Sum(ConstantDollarProduction), IntraSect5d_cons |  Sum(ConstantDollarProduction) - IntraSect5d_cons
										       Sum(ConstantDollarProduction), IntraSect4d_cons |  Sum(ConstantDollarProduction) - IntraSect4d_cons
											   Sum(ConstantDollarProduction), IntraSect3d_cons |  Sum(ConstantDollarProduction) - IntraSect3d_cons
											   Sum(ConstantDollarProduction), IntraSectSc_cons |  Sum(ConstantDollarProduction) - IntraSectSc_cons 	*/
Proc sql;
	Create table  	work.Sect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction	
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect5d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect4d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect3d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "SectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSectSc_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/* Calculate Sectoral Production Index  (Constant $) | Sect5d_cons | Calculate Index of Sect5d_cons
													   Sect4d_cons | Calculate Index of Sect4d_cons
										   			   Sect3d_cons | Calculate Index of Sect3d_cons
													   SectSc_cons | Calculate Index of SectSc_cons                                   */
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
quit;


/*Calculate Output Weighting Effect (T90)*/
Proc sql;
	Create table  	work.Sum_ConstantDollarProduction as 
    Select 			IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value
	from 			work.ConstantDollarProduction	
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;

	Create table  	work.AggInd_ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, "AggIndex_ConstantDollarProduction" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sum_ConstantDollarProduction a
	inner join		work.Sum_ConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and
					b.YearNo=1;

	Create table  	work.OutAdRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T90" as DataSeriesId, a.YearID, a.YearNo, (a.Value / b.Value) as Value
    from 	     	work.AnnOut a
	inner join		work.AggInd_ConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*************************************/
/**Sectoral Output Index (T11-T14)****/
/*************************************/
	
/*Calculate Sectoral Output	Index (T11-T14)  |   Index_Sect5d_cons, OutAdRat (output weighting effect) | Index_Sect5d_cons * OutAdRat
											     Index_Sect4d_cons, OutAdRat (output weighting effect) | Index_Sect4d_cons * OutAdRat
											     Index_Sect3d_cons, OutAdRat (output weighting effect) | Index_Sect3d_cons * OutAdRat
									        	 Index_SectSc_cons, OutAdRat (output weighting effect) | Index_SectSc_cons * OutAdRat	                */
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

	Create table 	work.SectOut as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect5d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect4d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_Sect3d_cons union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.OutIndex_SectSc_cons
	order by		IndustryID, DataSeriesID, YearID;
quit;


/*******************************************/
/*Sectoral Production (Current $) (T21-T24)*/
/*******************************************/

/*Calculate Sectoral Production (Current $) (T21-T24)  |  AnnVp, IntraSect5D |  AnnVP - IntraSect5D
											              AnnVp, IntraSect4D |  AnnVP - IntraSect4D
											              AnnVp, IntraSect3D |  AnnVP - IntraSect3D
									        	          AnnVp, IntraSectSc |  AnnVP - IntraSectSc                */
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

	Create table 	work.SectVal as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect5d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect4d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect3d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectSc
	order by		IndustryID, DataSeriesID, YearID;
quit;


/* Merging calculated variables together along with source data variables */
proc sql;
	Create table 	work.OutServiceCalcVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.InvChg union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraInd union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraSect union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdRat 
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutServiceCalcVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ServiceSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
