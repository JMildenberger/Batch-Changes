libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for mining. */

data work.MiningSource;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.OutputSource as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.MiningSource
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
	Out-MinPQ configuration */

Proc sql;
	Create table	work.OutMinPQ as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-MinPQ.sas";   
quit;

/*	The Year Number is extracted from the variable YearID	*/

data work.IPS_SourceData;
	retain IndustryID DataSeriesID DataArrayID YearID CensusPeriodID XLFormID YearNo Value;
	set work.OutMinPQ;
	YearNo=input(substr(YearID,5,1),1.);
	if Value = . then Value = 0;
run;

/* [Unadjusted] Annual Value of Production (i.e. not adjusted for benchmark ratio) | XT31 = Qnt, XT30 = Price | Sum(XT31*XT30) */

Proc sql;
	Create table  	work.Qnt as 
    Select          IndustryID, CensusPeriodID, DataArrayId, DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData
    where 			DataSeriesID="XT31";

	Create table  	work.Price as 
    Select          IndustryID, CensusPeriodID, DataArrayId, DataSeriesID, YearID, YearNo, Value
    from 	     	work.IPS_SourceData
    where 			DataSeriesID="XT30";

	Create table	work.ValProd_unadj_detail as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayId, "ValProd_unadj_detail" as Dataseries, 
					a.YearID, a.YearNo, a.Value*b.Value as Value
	from 			work.Qnt a 
	inner join		work.Price b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID) and (a.YearNo=b.YearNo);

	Create table	work.ValProd_unadj as 
	Select			IndustryID, CensusPeriodID, "ValProd_unadj" as Dataseries, YearID, YearNo, sum(Value) as Value
	from 			work.ValProd_unadj_detail
	group by		IndustryID, CensusPeriodID, YearID, YearNo ; 

quit;

/*	Shipments Adjustment Ratio | XT38 = VSInd, ValProd_unadj | (XT38/ValProd_unadj) */
Proc sql;
	Create table	work.ShipAdjRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "ShipAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.IPS_SourceData a 
	inner join 		work.ValProd_unadj b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and (a.DataSeriesID="XT38") ;
quit;

/*	Interpolation Macro	
	Multiple variables must be interpolated for non-Census years. The macro calculates the incremental value for the 
	interpolation and adds the inceremental value to the previous years value. If year 6 is not available dataset is empty 
	resulting in the final table having values set equal to Year 1 */

%macro Interpolate (SourceData, StructureData);
Proc sql;

	Create table  	work.Diff&SourceData as 
    Select          a.IndustryID, a.CensusPeriodID, (a.Value-b.Value)/5 as IncrementValue
    from 	     	&SourceData a
	inner join		&SourceData b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearNo=6) and (b.YearNo=1);

	Create table	work.Working&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, b.Value, 
					case 	when c.IncrementValue is null then 0 
							else c.IncrementValue 
					end 	as IncrementValue
	from			&StructureData a 
	left join 		&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) 
	left join 		Diff&SourceData c
	on 				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);

	Create table	work.Annual&SourceData as
	Select			a.IndustryID, a.CensusPeriodID, "Annual&SourceData" as Dataseries, a.YearID, a.YearNo,
					(a.IncrementValue*(a.YearNo-1))+b.Value as Value
	from			work.Working&SourceData a
	inner join		work.Working&SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);
quit;
%mend Interpolate;

/* 	The "ShipAdjRatio" must be interpolated for non-Census years. */

%interpolate(ShipAdjRatio, ValProd_unadj);

/**************************/
/*Value of Shipments (T40)*/
/**************************/

/*	T40 = ValShip | ValProd_unadj, AnnualShipAdjRatio | (T36 * AnnualShipAdjRatio) */
Proc sql;
	Create table	work.ValShip as 
	Select			a.IndustryID, a.CensusPeriodID, "T40" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.ValProd_unadj a 
	inner join 		work.AnnualShipAdjRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/************************************/
/*Annual change in inventories (T50)*/
/************************************/

/* [Unadjusted] Change in inventories | XT47 = InvEOY, XT14 = InvBOY | XT47-XT14 */
Proc sql;
	Create table  	work.InvChg_unadj as 
    Select          a.IndustryID, a.CensusPeriodID, "InvChg_unadj" as Dataseries, a.YearID, a.YearNo, 
					(a.Value-b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.IPS_SourceData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT47") and (b.DataSeriesID="XT14");

quit;
				
/* [Benchmark Years] Inventory Adjustment Ratio | InvChg_unadj, T40 = ValShip | (InvChg/T40) */

Proc sql;
	Create table	work.InvChgAdjRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "InvChgAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.InvChg_unadj a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/* 	The "InvChgAdjRatio" must be interpolated for non-Census years. */

%interpolate(InvChgAdjRatio, ValProd_unadj);

/*	T50 = InvChg | AnnualInvChgAdjRatio, T40 = ValShip | (AnnualInvChgAdjRatio* T40) */

Proc sql;
	Create table	work.InvChg as 
	Select			a.IndustryID, a.CensusPeriodID, "T50" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.AnnualInvChgAdjRatio a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/**********************/
/*Annual resales (T51)*/
/**********************/

/* [Unadjusted] Resales | XT43 = VSResale*/
Proc sql;
	Create table  	work.Resales_unadj as 
    Select          IndustryID, CensusPeriodID, "Resales_unadj" as Dataseries, YearID, YearNo, Value
    from 	     	work.IPS_SourceData
    where 			DataSeriesID="XT43";
quit;
				
/* [Benchmark Years Only] Resales Adjustment Ratio | Resales_unadj, T40 = ValShip | (Resales_unadj/T40) */

Proc sql;
	Create table	work.ResalesAdjRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "ResalesAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.Resales_unadj a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/* 	The "ResalesAdjRatio" must be interpolated for non-Census years. */

%interpolate(ResalesAdjRatio, ValProd_unadj);

/*	T51 = Resales | AnnualResalesAdjRatio, T40 = ValShip | (AnnualResalesAdjRatio* T40) */
Proc sql;
	Create table	work.Resales as 
	Select			a.IndustryID, a.CensusPeriodID, "T51" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.AnnualResalesAdjRatio a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/***************************/
/*Annual IntraInd (T52)*/
/***************************/

/* [Unadjusted] IntraIndustry Shipments | XT41 = VSIntra*/

Proc sql;
	Create table  	work.IntraInd_unadj as 
    Select          IndustryID, CensusPeriodID, "IntraInd_unadj" as Dataseries, YearID, YearNo, Value
    from 	     	work.IPS_SourceData
    where 			DataSeriesID="XT41";
quit;
				
/*	IntraIndustry Shipments Adjustment Ratio | IntraInd_unadj, T40 = ValShip | (IntraInd_unadj/T40) */

Proc sql;
	Create table	work.IntraIndAdjRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "IntraIndAdjRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.IntraInd_unadj a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/* 	The "IntraIndAdjRatio" must be interpolated for non-Census years. */

%interpolate(IntraIndAdjRatio, ValProd_unadj);

/*	T52 = Annual IntraInd | AnnualIntraIndAdjRatio, T40 = ValShip | (AnnualIntraIndAdjRatio* T40) */

Proc sql;
	Create table	work.IntraInd as 
	Select			a.IndustryID, a.CensusPeriodID, "T52" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.AnnualIntraIndAdjRatio a 
	inner join 		work.ValShip b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/***********************************/
/*Value of Annual Production (T36))*/
/***********************************/

/* T36 = Annual Value of Production | T40 = ValShip, InvChg, Resales, IntraInd  | (T40 + InvChg-Resales-IntraInd) */

Proc sql;
	Create table	work.AnnVP as 
	Select			a.IndustryID, a.CensusPeriodID, "T36" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value+b.Value-c.Value-d.Value) as Value
	from 			work.ValShip a 
	inner join 		work.InvChg b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo)
	inner join 		work.Resales c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and 
					(a.YearID=c.YearID) and (a.YearNo=c.YearNo)
	inner join 		work.IntraInd d
	on				(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID) and 
					(a.YearID=d.YearID) and (a.YearNo=d.YearNo);
quit;

/*********************************/
/*Value of Production Ratio (T93)*/
/*********************************/

/* T93 = VPRatio |  T36 = AnnVP , ValProd_unadj | (T36/ValProd_unadj) */

Proc sql;
	Create table	work.VPRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "T93" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
	from 			work.AnnVP a 
	inner join 		work.ValProd_unadj b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/********************/
/*Output Index (T37)*/
/********************/

/* [Benchmarked] Production Quantity | Qnt, VPRatio | (Qnt*VPRatio)  */

Proc sql;
	Create table	work.Qnt_bn as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataArrayId, "Benchmarked Production Quantity" as Dataseries, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
	from 			work.Qnt a 
	inner join 		work.VPRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/* [Benchmarked] Index of Production Quantity | Qnt_bn | Index of (Qnt_bn)   */

proc sql;
	Create table  	work.Index_Qnt_bn as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "Index of Benchmarked Production Quantity" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.Qnt_bn a
	inner join		work.Qnt_bn b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayId=b.DataArrayID) and b.YearNo=1;
quit;

/* Current Dollar Value of Production | Benchmarked Production Quantity, Prices | (Benchmarked Production Quantity * Prices) */

proc sql;
	Create table  	work.CurrentDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "CurrentDollarProduction" as Dataseries, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.Qnt_bn a
	inner join		work.Price b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.DataArrayID=b.DataArrayID)and
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/*Index of Prices | Price | (Price t+1 / Price t *100)   */

proc sql;
	Create table  	work.Index_Price as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "Index_Price" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.Price a
	inner join		work.Price b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataArrayId=b.DataArrayID) and b.YearNo=1;
quit;

/* Constant Dollar Value of Production | CurrentDollarProduction, Price Index | (CurrentDollarProduction / Price Index *100)   */

proc sql;
	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataArrayId, "ConstantDollarProduction" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.CurrentDollarProduction a
	inner join		work.Index_price b
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

/*	Calculating exponent of sum of weighted product growth rates | Exp (Sum(LogarithmicChange*AverageAnnualShares))*/

proc sql;
	Create table  	work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, "Exp Sum of weighted product growth rates" as Dataseries, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
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

/*Interpolated Intra-Sectoral Shipments Ratios | XT08=IntSect1 (Table_4_IntraSect_5Digit), ValProd_unadj (Annual Shipments) | XT08 / ValProd_unadj 
												 XT09=IntSect2 (Table_4_IntraSect_4Digit), ValProd_unadj (Annual Shipments) | XT09 / ValProd_unadj
												 XT10=IntSect3 (Table_4_IntraSect_3Digit), ValProd_unadj (Annual Shipments) | XT10 / ValProd_unadj
												 XT11=IntSect4 (Table_4_IntraSect_Sector), ValProd_unadj (Annual Shipments) | XT11 / ValProd_unadj */

Proc sql;
	Create table  	work.IntraSect5d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect5d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValProd_unadj b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT08");
				
	Create table  	work.IntraSect4d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect4d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValProd_unadj b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT09");

	Create table  	work.IntraSect3d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect3d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValProd_unadj b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT10");

	Create table  	work.IntraSectSc_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectSc_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValProd_unadj b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT11");
quit; 

%interpolate(IntraSect5d_rat, ValProd_unadj);
%interpolate(IntraSect4d_rat, ValProd_unadj);
%interpolate(IntraSect3d_rat, ValProd_unadj);
%interpolate(IntraSectSc_rat, ValProd_unadj);

/* Interpolated Intra-Sectoral Shipments (Current $) | ValProd_unadj, AnnualIntraSect5d_rat | ValProd_unadj * AnnualIntraSect5d_rat 
										  			   ValProd_unadj, AnnualIntraSect4d_rat | ValProd_unadj * AnnualIntraSect4d_rat
								     	  			   ValProd_unadj, AnnualIntraSect3d_rat | ValProd_unadj * AnnualIntraSect3d_rat
												 	   ValProd_unadj, AnnualIntraSectSc_rat | ValProd_unadj * AnnualIntraSectSc_rat   */

Proc sql;
	Create table  	work.IntraSect5d as 
    Select          a.IndustryID, a.CensusPeriodID, "T53" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValProd_unadj a
	inner join		work.AnnualIntraSect5d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.IntraSect4d as 
    Select          a.IndustryID, a.CensusPeriodID, "T54" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValProd_unadj a
	inner join		work.AnnualIntraSect4d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSect3d as 
    Select          a.IndustryID, a.CensusPeriodID, "T55" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValProd_unadj a
	inner join		work.AnnualIntraSect3d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectSc as 
    Select          a.IndustryID, a.CensusPeriodID, "T58" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValProd_unadj a
	inner join		work.AnnualIntraSectSc_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

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

/*Calculate Combined Price Deflator | Sum(CurrenttDollarProduction), Sum(ConstantDollarProduction) | Sum(CurrentDollarProduction) / 
                                                                                                     Sum(ConstantDollarProduction)*100    */

proc sql;
	Create table  	work.CombPriceDfl as 
    Select          a.IndustryID, a.CensusPeriodID, "CombPriceDfl" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.CurrentDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;

/* Calculate Intra-Sectoral Shipments (Constant $) | IntraSect5d, CombPriceDfl |  (IntraSect5d / CombPriceDfl)*100
										 		     IntraSect4d, CombPriceDfl |  (IntraSect4d / CombPriceDfl)*100
											 	     IntraSect3d, CombPriceDfl |  (IntraSect3d / CombPriceDfl)*100
											 	     IntraSectSc, CombPriceDfl |  (IntraSectSc / CombPriceDfl)*100                  	*/

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
													   SectSc_cons | Calculate Index of SectSc_cons */

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

/* T90 = OutAdRat */

Proc sql;
	Create table  	work.Sum_ConstantDollarProduction as 
    Select 			IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value
	from 			work.ConstantDollarProduction	
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;

	Create table  	work.Index_ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, "Index_ConstantDollarProduction" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sum_ConstantDollarProduction a
	inner join		work.Sum_ConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and
					b.YearNo=1;

	Create table  	work.OutAdRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T90" as DataSeriesId, a.YearID, a.YearNo, (a.Value / b.Value) as Value
    from 	     	work.AnnOut a
	inner join		work.Index_ConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*************************************/
/**Sectoral Output Index (T11-T14)****/
/*************************************/
	
/*Calculate Sectoral Output	Index (T11-T14)  |   Index_Sect5d_cons, OutAdRat (output weighting effect) | Index_Sect5d_cons * OutAdRat
											     Index_Sect4d_cons, OutAdRat (output weighting effect) | Index_Sect4d_cons * OutAdRat
											     Index_Sect3d_cons, OutAdRat (output weighting effect) | Index_Sect3d_cons * OutAdRat
									        	 Index_SectSc_cons, OutAdRat (output weighting effect) | Index_SectSc_cons * OutAdRat */

Proc sql;
	Create table  	work.Sect5dOut as 
    Select          a.IndustryID, a.CensusPeriodID, "T11" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect5d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4dOut as 
    Select          a.IndustryID, a.CensusPeriodID, "T12" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect4d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3dOut as 
    Select          a.IndustryID, a.CensusPeriodID, "T13" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_Sect3d_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectScOut as 
    Select          a.IndustryID, a.CensusPeriodID, "T14" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.Index_SectSc_cons a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table 	work.SectOut as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect5dOut union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect4dOut union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect3dOut union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectScOut
	order by		IndustryID, DataSeriesID, YearID;
quit;

/*******************************************/
/*Sectoral Production (Current $) (T21-T24)*/
/*******************************************/

/*Calculate Sectoral Production (Current $) (T21-T24)  |  Sum(CurrentDollarProduction), IntraSect5D | Sum(CurrentDollarProduction) - IntraSect5D
											              Sum(CurrentDollarProduction), IntraSect4D | Sum(CurrentDollarProduction) - IntraSect4D
											              Sum(CurrentDollarProduction), IntraSect3D | Sum(CurrentDollarProduction) - IntraSect3D
									        	          Sum(CurrentDollarProduction), IntraSectSc | Sum(CurrentDollarProduction) - IntraSectSc  */

Proc sql;
	Create table  	work.Sect5dVal as 
    Select          a.IndustryID, a.CensusPeriodID, "T21" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
    from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					 from 		work.CurrentDollarProduction
					 group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect5d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4dVal as 
    Select          a.IndustryID, a.CensusPeriodID, "T22" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
    from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					 from 		work.CurrentDollarProduction
					 group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect4d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3dVal as 
    Select          a.IndustryID, a.CensusPeriodID, "T23" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					 from 		work.CurrentDollarProduction
					 group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect3d b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectScVal as 
    Select          a.IndustryID, a.CensusPeriodID, "T24" as DataSeriesID, a.YearID, a.YearNo, (a.Value - b.Value) as Value
     from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					 from 		work.CurrentDollarProduction
					 group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSectSc b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


	Create table 	work.SectVal as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect5dVal union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect4dVal union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.Sect3dVal union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.SectScVal
	order by		IndustryID, DataSeriesID, YearID;
quit;

/* Merging calculated variables together along with source data variables */

proc sql;
	Create table 	work.OutMin8717CalculatedVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVp union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.InvChg union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.Resales union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraInd union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraSect union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdRat union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPRatio 
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutMin8717CalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.MiningSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
