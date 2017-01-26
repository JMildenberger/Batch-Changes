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
	Out-DV configuration */

Proc sql;
	Create table	work.OutDV as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-DV.sas";   
quit;


/*	The Year Number is extracted from the variable YearID	*/
data work.IPS_SourceData;
	set work.OutDV;
	YearNo=input(substr(YearID,5,1),1.);
	if Value = . then Value = 0;
run;


/**************
*ValShip (T40)*
***************/

/*	Industry Value of Shipments | XT39=VSInd |  XT39 */
proc sql;
	Create table	work.ValShip as
	Select 			IndustryID, CensusPeriodID, YearID, YearNo, "T40" as DataSeriesID, Value 
	from 			work.IPS_SourceData
	where			DataSeriesID = "XT39";
quit;


/***********************************************
*ValShipP (T41), ValShipS (T42), ValShipM (T43)*
************************************************/

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


/*	Primary Products Shipment Ratio | XT42 = VSPProd, ValShip |  XT42 / ValShipp  */
proc sql;
	Create table	work.PrimaryProductShipRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "PrimaryProductShipRatio" as Dataseries, a.YearID, a.YearNo,  
					(a.Value / b.Value) as Value
    from        	work.IPS_SourceData a
	inner join      work.ValShip b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo )and (a.DataSeriesID="XT42");
quit;

%Interpolate(PrimaryProductShipRatio, ValShip);


/*	Secondary Products Shipment Ratio | XT44 = VSSProd, ValShip |  XT44 / ValShip  */
proc sql;
	Create table	work.SecondaryProductShipRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "SecondaryProductShipRatio" as Dataseries, a.YearID, a.YearNo,  
					(a.Value / b.Value) as Value
    from        	work.IPS_SourceData a
	inner join      work.ValShip b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT44");
quit;

%Interpolate(SecondaryProductShipRatio, ValShip);


/*	Miscellaneous Receipts Ratio |     AnnualPrimaryProductShipRatio , AnnualSecondaryProductShipRatio       |  
                                   1 - AnnualPrimaryProductShipRatio - AnnualSecondaryProductShipRatio              */
proc sql;
	Create table	work.MiscReceiptsRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "MiscReceiptsRatio" as Dataseries, a.YearID, a.YearNo,  
					(1-a.Value-b.Value) as Value
    from        	work.AnnualPrimaryProductShipRatio a
	inner join      work.AnnualSecondaryProductShipRatio b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);
quit;


/*	Primary Product Value of Shipments | AnnualPrimaryProductShipRatio, ValShip | AnnualPrimaryProductShipRatio * ValShip	*/
proc sql;
	Create table  	work.ValShipP as 
    Select          a.IndustryID, a.CensusPeriodID, "T41" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.AnnualPrimaryProductShipRatio a
	inner join		work.ValShip b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*	Secondary Product Value of Shipments | AnnualSecondaryProductShipRatio, ValShip | AnnualSecondaryProductShipRatio * ValShip	*/
proc sql;
	Create table  	work.ValShipS as 
    Select          a.IndustryID, a.CensusPeriodID, "T42" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.AnnualSecondaryProductShipRatio a
	inner join		work.ValShip b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*	Miscellaneous Recepts | MiscRecShipRatio, ValShip | MiscRecShipRatio * ValShip	*/
proc sql;
	Create table  	work.ValShipM as 
    Select          a.IndustryID, a.CensusPeriodID, "T43" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.MiscReceiptsRatio a
	inner join		work.ValShip b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/***************
*IntraInd (T52)*
****************/

/*	Primary & Secondary Product Shipments | ValShipP, ValShipS | ValShipP + ValShipS	*/
proc sql;
	Create table  	work.ValShipPS as 
    Select          a.IndustryID, a.CensusPeriodID, "ValShip_Primary_Secondary" as DataSeries, a.YearID, a.YearNo, 
					(a.Value+b.Value) as Value
    from 	     	work.ValShipP a
	inner join		work.ValShipS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;
 
/*	Intra-Industry to Primary Plus Secondary Ratio | XT41 = VSIntra, ValShipPS | XT41 / ValShipPS	*/
proc sql;
	Create table  	work.IntraInd_ValShipPS_Ratio as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraInd_ValShipPS_Ratio" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and a.DataSeriesID="XT41";
quit;
 
%Interpolate(IntraInd_ValShipPS_Ratio, ValShip);

/*	Intra-Industry Shipments | AnnualIntraInd_ValShipPS_Ratio, ValShipPS | AnnualIntraInd_PrimSec_ratio * ValShipPS	*/
proc sql;
	Create table  	work.IntraInd as 
    Select          a.IndustryID, a.CensusPeriodID, "T52" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.AnnualIntraInd_ValShipPS_Ratio a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*************
*InvChg (T50)*
**************/

/*	Calculating InvBegYr | XT15=InvBOYFG, XT17=InvBOYWP | (XT15+XT17) */
proc sql;
	Create table  	work.InvBegYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,(a.Value+b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.IPS_SourceData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID = "XT15") and (b.DataSeriesID = "XT17");
quit;

/*	Calculating InvEndYr | XT18=InvEOYFG, XT20=InvEOYWP | (XT18+XT20) */
proc sql;
	Create table  	work.InvEndYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, (a.Value+b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.IPS_SourceData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID = "XT18") and (b.DataSeriesID = "XT20");
quit;

/*	Calculating InvChg (T50) | InvEndYr, InvBegYr | InvEndYr-InvBegYr */
proc sql;
	Create table  	work.InvChg as 
    Select          a.IndustryID, a.CensusPeriodID, "T50" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value-b.Value) as Value
    from 	     	work.InvEndYr a
	inner join		work.InvBegYr b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*******************************
*ValProdP (T31), ValProdS (T32)*
********************************/

/*	Primary Plus Secondary Value of Shipments (adjusted for IntraInd & InvChg) | ValShipPS, IntraInd, InvChg | ValShipPS + IntraInd + InvChg	*/
proc sql;
	Create table  	work.ValProdPS as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimarySecondaryProductProd" as DataSeries, a.YearID, a.YearNo, 
					(a.Value-b.Value+c.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.IntraInd b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) 
	inner join		work.InvChg c
   	on	 			(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and 
					(a.YearNo=c.YearNo);
quit;

/*	Primary Product Shipment Ratio | ValShipP, ValShipPS_adjusted |  ValShipP / ValShipPS 	*/
proc sql;
	Create table  	work.PrimaryProductRatio as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryProductRatio" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.ValShipP a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*	Secondary Product Shipment Ratio | ValShipS, ValShipPS_adjusted |  ValShipS / ValShipPS 	*/
proc sql;
	Create table  	work.SecondaryProductRatio as 
    Select          a.IndustryID, a.CensusPeriodID, "SecondaryProductRatio" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.ValShipS a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*	Primary Product Production | ValProdPS, PrimaryProductRatio |  ValProdPS * PrimaryProductRatio 	*/
proc sql;
	Create table  	work.ValProdP as 
    Select          a.IndustryID, a.CensusPeriodID, "T31" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.ValProdPS a
	inner join		work.PrimaryProductRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*	Secondary Product Production | ValProdPS, SecondaryProductRatio |  ValProdPS * SecondaryProductRatio 	*/
proc sql;
	Create table  	work.ValProdS as 
    Select          a.IndustryID, a.CensusPeriodID, "T32" as DataSeriesID, "01" as DataArrayID, 
					a.YearID, a.YearNo, (a.Value*b.Value) as Value, "XT46" as DeflMatch
    from 	     	work.ValProdPS a
	inner join		work.SecondaryProductRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/***************
*WhrEvCur (T46)*
****************/

/*	Calculating WhrEvCur (T46) which is the sum of wherever made product shipments | XT32=Sale | Sum(XT32) */
proc sql;
	Create table  	work.WhrEvCur as 
    Select          a.IndustryID, a.CensusPeriodID, "T46" as DataSeriesID, a.YearID, a.YearNo, sum(a.Value) as Value
    from 	     	work.IPS_SourceData a
    where 			a.DataSeriesID="XT32"
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;


/***************
*PPAdjRat (T92)*
****************/

/* Calculating PPAdjRat (T92) | ValProdP, WhrEvCur | ValProdP/WhrEvCur */
proc sql;
	Create table  	work.PPAdjRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T92" as DataSeriesID, a.YearID, a.YearNo, 
					a.Value/b.Value as Value
    from 	     	work.ValProdP a
	inner join		work.WhrEvCur b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);
quit;


/**************
*Resales (T51)*
**************/

/*	Resales to Miscellaneous Receipts Ratio | XT43 = VSResale, ValShipM| VSResale / ValShipM	*/
proc sql;
	Create table  	work.Resales_MiscReceipts_Rat as 
    Select          a.IndustryID, a.CensusPeriodID, "Resales_MiscReceipts_Ratio" as DataSeries, a.YearID, a.YearNo, 
					case 	when a.Value=b.Value then 1 
							else a.Value/b.Value 
					end		as Value
    from 	     	work.IPS_SourceData a 
	inner join		work.ValShipM b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and a.DataSeriesID="XT43";
quit;

%Interpolate(Resales_MiscReceipts_Rat, ValShip);

/*	Annual Resales (T51) | Resales_MiscReceipts_Rat, ValShipM  | Resales_MiscReceipts_Rat * ValShipM	*/
proc sql;
	Create table  	work.Resales as 
    Select          a.IndustryID, a.CensusPeriodID, "T51" as DataSeriesID, a.YearID, a.YearNo, 
					a.Value*b.Value as Value
    from 	     	work.AnnualResales_MiscReceipts_Rat a
	inner join		work.ValShipM b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);
quit;


/**************
*ValProdM (T33)*
**************/

/*	Miscellaneous Receipts Excluding Resales (T33) | ValShipM, Resales  | ValShipM - Resales	*/
proc sql;
	Create table  	work.ValProdM as 
    Select          a.IndustryID, a.CensusPeriodID, "T33" as DataSeriesID, "01" as DataArrayID, 
					a.YearID, a.YearNo, (a.Value-b.Value) as Value, "XT45" as DeflMatch 
    from 	     	work.ValShipM a
	inner join		work.Resales b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);
quit;


/************
*AnnVp (T36)*
*************/

/*	Annual Value of Production (T36) | ValShip, InvChg, IntraInd, Resales| ValShip + InvChg - IntraInd - Resales	*/
proc sql;
	Create table  	work.AnnVp as 
    Select          a.IndustryID, a.CensusPeriodID, "T36" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value+b.Value-c.Value-d.Value) as Value
    from 	     	work.ValShip a
	inner join		work.InvChg b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join		work.IntraInd c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and (a.YearID=c.YearID) and
					(a.YearNo=c.YearNo)
	inner join		work.Resales d
	on				(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID)and (a.YearID=d.YearID) and
					(a.YearNo=d.YearNo);
quit;


/*************
*AnnOut (T37)*
**************/

/*	Applying PPAdjRat to Sale Data (XT32) | 
	The variable DeflMatch is used to match production values with proper deflators (XT06=Defl) */
proc sql;
	Create table	work.CurrentPrimaryProductionData as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, "XT06" as DeflMatch, a.YearID, 
					a.YearNo,(a.Value*b.Value) as Value 
	from 			work.IPS_SourceData a
	inner join		work.PPAdjRat b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and a.DataSeriesID="XT32";
quit;

/*	Merging production data together for Torqnvist process */
proc sql;
	Create table	work.AllCurrentProductionData as 
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			CurrentPrimaryProductionData 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			ValProdM 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			ValProdS;
quit;

/*	Querying deflator data together for Torqnvist process | XT06=Defl, XT45=DeflMisc, XT46=DeflSecd */
proc sql;
	Create table  	work.AllDeflatorData as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.DataArrayID, a.YearID, a.YearNo, a.Value
    from 	     	work.IPS_SourceData a
    where 			(a.DataSeriesID="XT06" or a.DataSeriesID="XT45" or a.DataSeriesID="XT46");
quit;

/*	 Rebasing deflator data to Year1 */
proc sql;
	Create table  	work.RebasedDeflators as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID,a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllDeflatorData a
	inner join		work.AllDeflatorData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and (a.DataArrayID=b.DataArrayID) and b.YearNo=1;
quit;

/*	Deflating Current Dollar Production | AllCurrentProductionData, RebasedDeflators | AllCurrentProductionData / RebasedDeflators */
proc sql;
	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllCurrentProductionData a	
	inner join		work.RebasedDeflators b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DeflMatch=b.DataseriesID)and (a.DataArrayID=b.DataArrayID) and(a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs		*/
proc sql;
	Create table  	work.Sub_ConstantDollarProduction as 
    Select          IndustryID, CensusPeriodID, DataseriesID, DataArrayID, YearID, YearNo,					 
					case when value = 0 then 0.001
						 else value
					end as value
    from 	     	work.ConstantDollarProduction ;

/*	Calculating logarithmic change in ConstantDollarProduction */

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
quit;

/*	Calculating average annual product shares of Current Dollar Production */
proc sql;
	Create table  	work.AverageAnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					(a.value+b.value)/2 as value
    from 	     	work.AnnualShares a 
	left join 		work.AnnualShares b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);
quit;

/*	Calculating exponent of sum of weighted product growth rates | Exp (Sum(LogarithmicChange*AverageAnnualShares))*/
proc sql;
	Create table  	work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
    from 	     	work.LogarithmicChange a
	inner join		work.AverageAnnualShares b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID) and 
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

/*Interpolated Intra-Sectoral Shipments Ratios | XT08=IntSect1 (Table_4_IntraSect_5Digit), ValShipPS (Primary & Secondary Product Shipments) | XT08 / ValShipPS 
												 XT09=IntSect2 (Table_4_IntraSect_4Digit), ValShipPS (Primary & Secondary Product Shipments) | XT09 / ValShipPS
												 XT10=IntSect3 (Table_4_IntraSect_3Digit), ValShipPS (Primary & Secondary Product Shipments) | XT10 / ValShipPS
												 XT11=IntSect4 (Table_4_IntraSect_Sector), ValShipPS (Primary & Secondary Product Shipments) | XT11 / ValShipPS
												 XT12=IntSect5 (Table_4_IntraSect_Combo1), ValShipPS (Primary & Secondary Product Shipments) | XT12 / ValShipPS
												 XT13=IntSect6 (Table_4_IntraSect_Combo2), ValShipPS (Primary & Secondary Product Shipments) | XT13 / ValShipPS  */
Proc sql;
	Create table  	work.IntraSect5d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect5d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT08");
				
	Create table  	work.IntraSect4d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect4d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT09");

	Create table  	work.IntraSect3d_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSect3d_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT10");

	Create table  	work.IntraSectSc_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectSc_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT11");

	Create table  	work.IntraSectC1_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectC1_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT12");

	Create table  	work.IntraSectC2_rat as 
    Select          a.IndustryID, a.CensusPeriodID, "IntraSectC2_rat" as DataSeries, a.YearID, a.YearNo, 
					(a.Value/b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.ValShipPS b 
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT13");

quit; 

%interpolate(IntraSect5d_rat, ValShip);
%interpolate(IntraSect4d_rat, ValShip);
%interpolate(IntraSect3d_rat, ValShip);
%interpolate(IntraSectSc_rat, ValShip);
%interpolate(IntraSectC1_rat, ValShip);
%interpolate(IntraSectC2_rat, ValShip);


/* Interpolated Intra-Sectoral Shipments (Current $) | ValShipPS, AnnualIntraSect5d_rat | ValShipPS * AnnualIntraSect5d_rat 
										  			   ValShipPS, AnnualIntraSect4d_rat | ValShipPS * AnnualIntraSect4d_rat
								     	  			   ValShipPS, AnnualIntraSect3d_rat | ValShipPS * AnnualIntraSect3d_rat
												 	   ValShipPS, AnnualIntraSectSc_rat | ValShipPS * AnnualIntraSectSc_rat
													   ValShipPS, AnnualIntraSectC1_rat | ValShipPS * AnnualIntraSectC1_rat 
													   ValShipPS, AnnualIntraSectC2_rat | ValShipPS * AnnualIntraSectC2_rat   */
Proc sql;
	Create table  	work.IntraSect5d as 
    Select          a.IndustryID, a.CensusPeriodID, "T53" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSect5d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.IntraSect4d as 
    Select          a.IndustryID, a.CensusPeriodID, "T54" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSect4d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSect3d as 
    Select          a.IndustryID, a.CensusPeriodID, "T55" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSect3d_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectSc as 
    Select          a.IndustryID, a.CensusPeriodID, "T58" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSectSc_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectC1 as 
    Select          a.IndustryID, a.CensusPeriodID, "T56" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSectC1_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.IntraSectC2 as 
    Select          a.IndustryID, a.CensusPeriodID, "T57" as DataSeriesID, a.YearID, a.YearNo, (a.Value * b.Value) as Value
    from 	     	work.ValShipPS a
	inner join		work.AnnualIntraSectC2_rat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table 	work.IntraSect as
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect5d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect4d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSect3d union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectSc union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectC1 union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, YearID, YearNo, Value 	from work.IntraSectC2
	order by		IndustryID, DataSeriesID, YearID;
quit;


/*****************/
/***OutAdRat(T90)*/
/*****************/

/*  Calculating PrimaryPlusSecondaryDeflator | PrimaryPlusSecondaryProduction , ConstantDollarProduction | 
											   PrimaryPlusSecondaryProduction / ConstantDollarProduction * 100       */
proc sql;
	Create table  	work.PrimaryPlusSecondaryDeflator as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryPlusSecondaryDeflator" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.ValProdPS a
	inner join		(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(value) as Value 
					from 		work.ConstantDollarProduction b 
					where 		DataSeriesID <> "T33" 
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*  Deflating IntraSectoralShipments with PrimaryPlusSecondaryDeflator | IntraSect , PrimaryPlusSecondaryDeflator | 
																		 IntraSect / PrimaryPlusSecondaryDeflator * 100    */
proc sql;
	Create table  	work.ConstantIntraSectoralShipments as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, a.YearNo,(a.Value/b.Value*100) as Value
    from 	     	work.IntraSect a
	inner join		work.PrimaryPlusSecondaryDeflator b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and  (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/* Summing ConstantDollarProduction for entire industry */
proc sql;
	Create table  	work.ConstantDollarTotal as 
	Select 			IndustryID, CensusPeriodID, YearID, YearNo, sum(value) as Value 
	from 			work.ConstantDollarProduction 
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;
quit;

/* Indexing ConstantDollarProduction for entire industry to YearNo 1*/
proc sql;
	Create table  	work.ConstantDollarProdIdx as 
    Select          a.IndustryID, a.CensusPeriodID, "ConstantDollarProdIdx" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.ConstantDollarTotal a
	inner join		work.ConstantDollarTotal b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (b.YearNo=1);
quit;

/*	Calculating OutAdRat (T90) - Output weighting effect | AnnOut , ConstantDollarProdIdx | 
														   AnnOut / ConstantDollarProdIdx 		*/
proc sql;
	Create table  	work.OutAdRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T90" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value) as Value
    from 	     	work.AnnOut a
	inner join		work.ConstantDollarProdIdx b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*************************************/
/**Sectoral Output Index (T11-T16)****/
/*************************************/

/* 	Calculating SectoralConstantDollarProduction | ConstantDollarTotal , ConstantIntraSectoralShipments |
												   ConstantDollarTotal - ConstantIntraSectoralShipments 	*/
proc sql;
	Create table  	work.SectoralConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, 
					a.YearNo,(b.Value-a.Value) as Value
    from 	     	work.ConstantIntraSectoralShipments a
	inner join		work.ConstantDollarTotal b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/*	Indexing SectoralConstantDollarProduction to Year No 1 */
proc sql;
	Create table  	work.SectoralConstantProductionIndex as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.SectoralConstantDollarProduction a
	inner join		work.SectoralConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and b.YearNo=1;
quit;

/* 	Calculating Sectoral Output Indexes | SectoralConstantProductionIndex , OutAdRat |
										  SectoralConstantProductionIndex * OutAdRat | 
										  T11=Sect5dOut, T12=Sect4dOut,  T13=Sect3dOut, 
										  T14=SectScOut, T15=SectC1COut, T16=SectC2Out	 	*/
proc sql;
	Create table  	work.SectOut as 
    Select          a.IndustryID, a.CensusPeriodID, 
					case 	when a.DataSeriesID="T53" then "T11"
							when a.DataSeriesID="T54" then "T12"
							when a.DataSeriesID="T55" then "T13"
							when a.DataSeriesID="T58" then "T14"
							when a.DataSeriesID="T56" then "T15"
							when a.DataSeriesID="T57" then "T16"
					end		as DataSeriesID, a.YearID, a.YearNo,(a.Value*b.Value) as Value			
    from 	     	work.SectoralConstantProductionIndex a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*******************/
/**WhrEvDfl (T66)**/
/******************/

/* 	Calculating WhrEvDfl (T66) |     AllCurrentProductionData  ,     ConstantDollarProduction |
							   	 Sum(AllCurrentProductionData) / Sum(ConstantDollarProduction) * 100 	*/
proc sql;
	Create table  	work.WhrEvDfl as 
    Select          a.IndustryID, a.CensusPeriodID, "T66" as DataSeriesID, a.YearID, a.YearNo,(
					a.Value/b.Value*100) as Value
    from 	     	(Select 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID, 
								sum(Value) as Value 
					from 		work.AllCurrentProductionData
					group by 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID) a
	inner join		(Select 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID, 
								sum(Value) as Value 
					from 		work.ConstantDollarProduction
					group by 	IndustryID, CensusPeriodID, YearID, YearNo, DataSeriesID) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and (a.YearID=b.YearID) and (a.YearNo=b.YearNo) and 
					(a.DataSeriesID="XT32");
quit;


/*******************/
/**WhrEvCon (T47)**/
/******************/

/* 	Calculating WhrEvCon (T47) | WhrEvCur, WhrEvDfl | WhrEvCur/WhrEvDfl*100 */
proc sql;
	Create table  	work.WhrEvCon as 
    Select          a.IndustryID, a.CensusPeriodID, "T47" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.WhrEvCur a
	inner join		work.WhrEvDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;


/*******************************************/
/*Sectoral Production (Current $) (T21-T26)*/
/*******************************************/

/*	Removing intrasectoral shipments from valprod to calculate sectoral production values | Sum(AllCurrentProductionData - IntraSect |
																							T21=Sect5dVal, T22=Sect4dVal, T23=Sect3dVal, 
																							T24=SectScVal, T25=SectC1CVal, T26SectC2Val= */
proc sql;
	Create table  	work.SectVal as 
    Select          a.IndustryID, a.CensusPeriodID, 
					case 	when a.DataSeriesID="T53" then "T21"
							when a.DataSeriesID="T54" then "T22"
							when a.DataSeriesID="T55" then "T23"
							when a.DataSeriesID="T58" then "T24"
							when a.DataSeriesID="T56" then "T25"
							when a.DataSeriesID="T57" then "T26"
					end		as DataSeriesID, a.YearID, a.YearNo,(b.value-a.Value) as Value					
    from 	     	work.IntraSect a
 	inner join 	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, 
								sum(Value) as Value 
					from 		work.AllCurrentProductionData
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
quit;

/* Merging calculated variables together along with source data variables */
proc sql;
	Create table 	work.OutDVCalculatedVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdS union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdM union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShipP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShipS union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShipM union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.WhrEvCur union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.WhrEvCon union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.InvChg union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.Resales union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraInd union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraSect union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.WhrEvDfl union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdRat union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.PPAdjRat
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutDVCalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ServiceSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;