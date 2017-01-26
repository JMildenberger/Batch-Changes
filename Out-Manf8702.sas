libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for manufacturing. */

data work.ManufacturingSource;
	set LPAll.Ips_SourceData_Extract;
run;

Proc sql;
	Create table 	work.OutputSource as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.ManufacturingSource
	where 			substr(DataSeriesID,1,2)="XT"
	order by 		IndustryID, DataSeriesID, YearID;
quit;

/*	This query isolates the configuration concordance stored in IPS to just Output */

Proc sql;
	Create table	work.ConfigDistinct as											
	Select 			IndustryID, IndustrySeriesID, CensusPeriodID, Program, Method	        
	from 			LPAll.ProgramMethodControlTable                                
	where 			IndustrySeriesID="Output";
quit;

/* 	This query uses the configuration concordance to filter only Industry/CensusPeriodIDs that use the 
	ManfOut8702 configuration */

Proc sql;
	Create table	work.OutManf8702 as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-Manf8702.sas";   
quit;

/*	The Year Number is extracted from the variable YearID	*/

data work.OutManf8702;
	set work.OutManf8702;
	YearNo=input(substr(YearID,5,1),1.);
run;

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

/* 	This query returns the unique combinations of IndustryID and YearID and serves as the structure dataset for 
	the interpolation macro. */

proc sql;
	create table 	work.IndYears as
	select			 distinct IndustryID, YearID, CensusPeriodID, YearNo
	from			work.OutManf8702;
quit;

/*	Ratio of Annual Industry Shipments to Census Year Product Shipments | XT39=VSIndAnn, XT40=VSIndCen | XT39/XT40 */

proc sql;
	Create table	work.IndShipAdjRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "IndShipAdjRatio" as Dataseries, a.YearID, a.YearNo, a.Value/b.Value as Value
    from        	work.OutManf8702 a
	inner join      work.OutManf8702 b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT39" or a.DataSeriesID="XT38") and (b.DataSeriesID="XT40");
quit;
%Interpolate (IndShipAdjRatio, IndYears);

/*	Ratio of Primary Product Shipments to Total Census Year Product Shipments | XT42=VSPProd, XT40=VSIndCen | XT42 / XT40 */

proc sql;
	Create table	work.PrimaryProductRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "PrimaryProductShipRatio" as Dataseries, a.YearID, a.YearNo, 
					a.Value/b.Value as Value
    from        	work.OutManf8702 a
	inner join      work.OutManf8702 b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT42") and (b.DataSeriesID="XT40");
quit;
%Interpolate (PrimaryProductRatio, IndYears);

/*	Ratio of Secondary Product Shipments to Total Census Year Product Shipments | XT44=VSSProd, XT40=VSIndCen | XT44 / XT40 */

proc sql;
	Create table	work.SecondaryProductRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "SecondaryProductShipRatio" as Dataseries, a.YearID, a.YearNo, 
					a.Value/b.Value as Value
    from        	work.OutManf8702 a
	inner join      work.OutManf8702 b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT44") and (b.DataSeriesID="XT40");
quit;
%Interpolate (SecondaryProductRatio, IndYears);

/*	Calculating Census Year Miscellaneous Receipts | XT40=VSIndCen, XT42=VSPProd, XT44=VSSProd | XT40-XT42-XT44 */

Proc sql;
	Create table	work.MiscReceipts as 
	Select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, a.Value - b.Value - c.Value as Value
	from 			work.OutManf8702 a 
	inner join 		work.OutManf8702 b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.DataSeriesID="XT40") and (b.DataSeriesID="XT42")
	inner join 		work.OutManf8702 c
	on				(b.IndustryID=c.IndustryID) and (b.CensusPeriodID=c.CensusPeriodID) and (b.YearID=c.YearID) and 
					(c.DataSeriesID="XT44");

/*	MiscReceiptsRatio | MiscReceipts, XT40=VSIndCen | MiscReceipts / XT40 */

	Create table	work.MiscReceiptsRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "MiscReceiptsRatio" as Dataseries, a.YearID, a.YearNo, 
					a.Value/b.Value as Value
    from        	work.MiscReceipts a
	inner join      work.OutManf8702 b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and (b.DataSeriesID="XT40");
quit;
%Interpolate (MiscReceiptsRatio, IndYears);

/* Calculating ValShip (T40) | XT39=VSIndAnn | T40=XT39 */
Proc sql;
	Create table	work.ValShip as 
	Select			IndustryID, CensusPeriodID, "T40" as DataseriesID, "ValShip" as Dataseries, YearID, YearNo, Value
	from 			work.OutManf8702 a 
	where			DataSeriesID="XT39" or DataSeriesID="XT38";
quit;

/*	Calculating ValShipP (T41) | XT39=VSIndAnn, AnnualPrimaryProductRatio | XT39*AnnualPrimaryProductRatio */

Proc sql;
	Create table	work.ValShipP as 
	Select			a.IndustryID, a.CensusPeriodID, "T41" as DataseriesID, "ValShipP" as Dataseries, a.YearID, a.YearNo, 
					a.Value * b.Value as Value
	from 			work.OutManf8702 a 
	inner join 		work.AnnualPrimaryProductRatio b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="XT39" or a.DataSeriesID="XT38");

/*	Calculating ValShipS (T42) | XT39=VSIndAnn, AnnualSecondaryProductRatio | XT39*AnnualSecondaryProductRatio */

	Create table	work.ValShipS as 
	Select			a.IndustryID, a.CensusPeriodID, "T42" as DataseriesID, "ValShipS" as Dataseries, a.YearID, a.YearNo, a.Value * b.Value as Value
	from 			work.OutManf8702 a 
	inner join 		work.AnnualSecondaryProductRatio b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="XT39" or a.DataSeriesID="XT38");

/*	Calculating ValShipM (T43) | XT39=VSIndAnn, AnnualMiscReceiptsRatio | XT39*AnnualMiscReceiptsRatio */

	Create table	work.ValShipM as 
	Select			a.IndustryID, a.CensusPeriodID, "T43" as DataseriesID, "ValShipM" as Dataseries, a.YearID, a.YearNo, a.Value * b.Value as Value
	from 			work.OutManf8702 a 
	inner join 		work.AnnualMiscReceiptsRatio b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID="XT39" or DataSeriesID="XT38");

/*	Primary Plus Secondary Product Shipments | ValShipP + ValShipS */

	Create table  	work.PrimaryPlusSecondaryProdShip as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryPlusSecondaryProdShip" as Dataseries, a.YearID, a.YearNo, 
					a.Value+b.Value as Value
    from 	     	work.ValShipP a 
	inner join 		work.ValShipS b
    on	 			(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);

/*	IntraInd (T52) | XT41=VSIntra */

	Create table  	work.IntraInd as 
    Select          IndustryID, CensusPeriodID, "T52" as DataSeriesID, YearID, YearNo, Value
    from 	     	work.OutManf8702
    where			(DataSeriesID = "XT41");

/*	Calculating InvBegYr | XT15=InvBOYFG, XT17=InvBOYWP |(XT15+XT17) */

	Create table  	work.InvBegYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, 
					case when a.method = "NoInvFG" then (0 + b.Value)
						 when a.method = "NoInv" then (0 + 0) 
						 else (a.Value+b.Value) 
					end as Value
    from 	     	work.OutManf8702 a 
	inner join 		work.OutManf8702 b 
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and (a.YearNo=b.YearNo) and
					(a.DataSeriesID = "XT15") and (b.DataSeriesID = "XT17");

/*	Calculating InvEndYr | XT18=InvEOYFG, XT20=InvEOYWP | (XT18+XT20)   				*/

	Create table  	work.InvEndYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, 
					case when a.method = "NoInvFG" then (0 + b.Value)
						 when a.method = "NoInv" then (0 + 0)
   						 else (a.Value+b.Value) 
					end as Value
    from 	     	work.OutManf8702 a 
	inner join 		work.OutManf8702 b 
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.YearNo=b.YearNo) and
					(a.DataSeriesID = "XT18") and (b.DataSeriesID = "XT20");

/*	Calculating InvChg (T50) | InvEndYr, InvBegYr | InvEndYr-InvBegYr */

	Create table  	work.InvChg as 
    Select          a.IndustryID, a.CensusPeriodID, "T50" as DataseriesID, "InvChg" as Dataseries, a.YearID, a.YearNo, (a.Value-b.Value) as Value
    from 	     	work.InvEndYr a 
	inner join 		work.InvBegYr b
    on	 			(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);

/*	Calculating Primary Plus Secondary Production | PrimaryPlusSecondaryProdShip - IntraInd + InvChg  */

	create table 	work.PrimaryPlusSecondaryProduction as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, "PrimaryPlusSecondaryProd" as Dataseries, a.YearNo, a.Value-b.Value+c.Value as Value
	from			work.PrimaryPlusSecondaryProdShip a 
	inner join 		work.IntraInd b 
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID)
	inner join 		work.InvChg c
	on				(b.IndustryID=c.IndustryID) and (b.CensusPeriodID=c.CensusPeriodID) and (b.YearID=c.YearID);

/*	Calculating Primary Product Specialization Ratio | ValShipP / PrimaryPlusSecondaryProdShip  */

	create table	work.PPSpecRatio as
	select			a.IndustryID, a.CensusPeriodID, "PPSpecRatio" as Dataseries, a.YearID, b.Value/a.Value as Value
	from			work.PrimaryPlusSecondaryProdShip a 
	inner join 		work.ValShipP b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating Secondary Product Ratio | ValShipS / PrimaryPlusSecondaryProdShip  */

	create table	work.SPRatio as
	select			a.IndustryID, a.CensusPeriodID, "SPRatio" as Dataseries, a.YearID, b.Value/a.Value as Value
	from			work.PrimaryPlusSecondaryProdShip a 
	inner join 		work.ValShipS b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating ValProdP (T31) | PrimaryPlusSecondaryProduction * PPSpecRatio */

	create table 	work.ValProdP as
	select			a.IndustryID, a.CensusPeriodID, "T31" as DataseriesID, "ValProdP" as Dataseries, "01" as DataArrayID, a.YearID, a.YearNo, 
					a.Value*b.Value as Value
	from			work.PrimaryPlusSecondaryProduction a 
	inner join 		work.PPSpecRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating ValProdS (T32) | PrimaryPlusSecondaryProduction * SPRatio */

	create table 	work.ValProdS as
	select			a.IndustryID, a.CensusPeriodID, "T32" as DataseriesID, "ValProdS" as Dataseries, "01" as DataArrayID, a.YearID, a.YearNo,
					"XT46" as DeflMatch, a.Value*b.Value as Value
	from			work.PrimaryPlusSecondaryProduction a 
	inner join 		work.SPRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating WhrEvCur (T46) which is the sum of wherever made product shipments | XT32=Sale | Sum(XT32) */

	Create table  	work.WhrEvCur as 
    Select          a.IndustryID, a.CensusPeriodID, "T46" as DataSeriesID, a.YearID, a.YearNo, sum(a.Value) as Value
    from 	     	work.OutManf8702 a
    where 			a.DataSeriesID="XT32"
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;

/*	Calculating PPAdjRat (T92) | ValProdP, WhrEvCur | ValProdP/WhrEvCur */

	create table	work.PPAdjRat as
	select			a.IndustryID, a.CensusPeriodID, "T92" as DataseriesID, "PPAdjRat" as Dataseries, a.YearID, a.YearNo, a.Value/b.Value as Value
	from			work.ValProdP a 
	inner join 		work.WhrEvCur b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Adjusting Census Year Resales |  AnnualIndShipAdjRatio, XT43=VSResale | AnnualIndShipAdjRatio*XT43 */

	create table	work.ResalesCenYrsAdj as
	select			a.IndustryID, a.CensusPeriodID, "ResalesCenYrsAdj" as Dataseries, a.YearID, a.YearNo, a.Value*b.Value as Value
	from			work.AnnualIndShipAdjRatio a 
	inner join 		work.OutManf8702 b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and (b.DataseriesID="XT43");

/*	Ratio of Resales to Miscellaneous Receipts | ResalesCenYrsAdj/ValShipM */

	create table	work.ResalesMiscRecRatio as
	select			a.IndustryID, a.CensusPeriodID, "ResalesMiscRecRatio" as Dataseries, a.YearID, a.YearNo, 
					case 	when a.Value=b.Value then 1 
							else a.Value/b.Value 
					end		as Value
	from			work.ResalesCenYrsAdj a
	inner join		work.ValShipM b
	on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);
quit;

/* 	The "ResalesToMiscReceiptsRatio" must be interpolated for non-Census years.*/
%Interpolate (ResalesMiscRecRatio, IndYears);

/*	Calculating Resales (T51) | ValShipM*AnnualResalesMiscRecRatio */

proc sql;
	Create table 	work.Resales as
	select			a.IndustryID, a.CensusPeriodID, "T51" as DataseriesID, "Resales" as Dataseries, a.YearID, a.YearNo, a.Value * b.Value as Value
	from			work.ValShipM a
	inner join		work.AnnualResalesMiscRecRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating ValProdM (T33) | ValShipM, Resales | ValShipM-Resales |
	The variable DeflMatch is used to match production values with proper deflators (XT45=DeflMisc) */

	create table	work.ValProdM as
	select			a.IndustryID, a.CensusPeriodID, "T33" as DataseriesID, "ValProdM" as Dataseries, "01" as DataArrayID, a.YearID, a.YearNo,
					"XT45" as DeflMatch, a.Value-b.Value as Value
	from			work.ValShipM a 
	inner join		work.Resales b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID);

/*	Calculating AnnVP (T36) | ValShip+InvChg-IntraInd-Resales */

	create table	work.AnnVP as
	select			a.IndustryID, a.CensusPeriodID, "T36" as DataseriesID, "AnnVP" as Dataseries, a.YearID, a.YearNo, "XT06" as DeflMatch, 
					a.Value+b.Value-c.Value-d.Value as Value
	from			work.ValShip a 
	inner join 		work.InvChg b
	on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID)
	inner join 		work.IntraInd c
	on				(b.IndustryID=c.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearID=c.YearID)
	inner join 		work.Resales d
	on				(c.IndustryID=d.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (c.YearID=d.YearID);

/*	Applying PPAdjRat to Sale Data (XT32) | 
	The variable DeflMatch is used to match production values with proper deflators (XT06=Defl) */

	Create table	work.CurrentPrimaryProductionData as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, "XT06" as DeflMatch, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value 
	from 			work.OutManf8702 a
	inner join		work.PPAdjRat b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and (a.DataSeriesID="XT32")
	order by 		DataArrayID, YearID;

/*	Merging production data together for Torqnvist process */

proc sql;
	Create table	work.AllCurrentProductionData as 
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			work.CurrentPrimaryProductionData a
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			work.ValProdM b 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			work.ValProdS;

/*	Querying deflator data together for Torqnvist process | XT06=Defl, XT45=DeflMisc, XT46=DeflSecd */

	Create table  	work.AllDeflatorData as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.DataArrayID, a.YearID, a.YearNo, a.Value
    from 	     	work.OutManf8702 a
    where 			(a.DataSeriesID="XT06" or a.DataSeriesID="XT45" or a.DataSeriesID="XT46");

/*	 Rebasing deflator data to Year1 */

	Create table  	work.RebasedDeflators as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID,a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllDeflatorData a
	inner join		work.AllDeflatorData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and (a.DataArrayID=b.DataArrayID) and b.YearNo=1;

quit;


/*  Calculate implicit primary deflator using Physical Quantity Methodolgoy*/
	Proc sql;

	Create table	work.PrimaryImPrDef2 as
	Select			a.IndustryID, a.CensusPeriodID, "XT06" as DataSeriesID, a.DataArrayID, a.YearID, a.YearNo,
					case	when 	a.YearNo=1 then 100
							else	a.Value/b.Value*100 
					end as value

	from			(Select 	a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, 
								a.YearNo, a.Value/b.Value as Value 
					from 		work.CurrentPrimaryProductionData a
					inner join 	work.CurrentPrimaryProductionData b 
					on			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
								(a.DataArrayID=b.DataArrayID)and(b.YearNo=1)) a

	inner join   	(Select		a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, 
								a.YearNo, a.Value/b.Value as Value 
					from 		work.OutManf8702 a
					inner join 	work.OutManf8702 b 
					on			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
								(a.DataArrayID=b.DataArrayID)and (a.DataSeriesID="XT31") and (b.DataSeriesID="XT31") and 
								(b.YearNo=1) and (a.Method="Physical")) b

	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataArrayID=b.DataArrayID);

	Create table	work.PrimaryImPrDef3 as
	Select			a.IndustryID, a.CensusPeriodID, "XT06" as DataSeriesID, a.DataArrayID, a.YearID, a.YearNo,
					case	when 	a.YearNo=1 then 100
							else	a.Value/b.Value*100 
					end as value

	from			(Select 	a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, 
								a.YearNo, a.Value/b.Value as Value 
					from 		work.OutManf8702 a
					inner join 	work.OutManf8702 b 
					on			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
								(a.DataArrayID=b.DataArrayID)and (a.DataSeriesID="XT32") and (b.DataSeriesID="XT32") and
								(b.YearNo=1)) a

	inner join   	(Select		a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, 
								a.YearNo, a.Value/b.Value as Value 
					from 		work.OutManf8702 a
					inner join 	work.OutManf8702 b 
					on			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
								(a.DataArrayID=b.DataArrayID)and (a.DataSeriesID="XT31") and (b.DataSeriesID="XT31") and 
								(b.YearNo=1) and (a.Method="PhysicalShipQnt")) b

	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataArrayID=b.DataArrayID);

	quit;

Proc sql;
	Create table	work.MergedDeflators as
	Select			* 
	from			work.RebasedDeflators union all
	Select			*
	from			work.PrimaryImPrDef2 union all
	Select			*
	from			work.PrimaryImPrDef3;

quit;


/*	Deflating Current Dollar Production | AllCurrentProductionData, RebasedDeflators | 
	AllCurrentProductionData/RebasedDeflators */

Proc sql;
	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllCurrentProductionData a	
	inner join		work.MergedDeflators b
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

/* 	Indexing AnnVP to YearNo=1*/

	create table	work.AnnVPIdx as
	select          a.IndustryID, a.CensusPeriodID, "AnnVPIdx" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.AnnVP a
	inner join		work.AnnVP b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (b.YearNo=1);


/*	Calculating implicit price deflator | AnnVPidx, AnnOut | AnnVPidx/AnnOut*100 */

	Create table  	work.ImpPrDef as 
    Select          a.IndustryID, a.CensusPeriodID, "ImPrDef" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.AnnVPIdx a
	inner join		work.AnnOut b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Creating a dataset of intrasectoral shipments | XT08=IntSect1, XT09=IntSect2, XT10=IntSect3, XT11=IntSect4,
	 | T53=IntraSect5d, T54=IntraSect4d, T55=IntraSect3d, T58=IntraSectSc */

	Create table  	work.IntraSect as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, a.Value,
					case 	when a.DataSeriesID="XT08" then "T53"
							when a.DataSeriesID="XT09" then "T54"
							when a.DataSeriesID="XT10" then "T55"
							when a.DataSeriesID="XT11" then "T58"
					end		as DataSeriesID	
    from 	     	work.OutManf8702 a
    where	 		(a.DataSeriesID = "XT08" or a.DataSeriesID = "XT09" or a.DataSeriesID = "XT10" or a.DataSeriesID = "XT11");


/*	Removing intrasectoral shipments from AnnVP to calculate sectoral production values | AnnVP - IntraSect |
	T53=IntraSect5d, T54=IntraSect4d, T55=IntraSect3d, T58=IntraSectSc 
	T21=Sect5dVal, T22=Sect4dVal, T23=Sect3dVal, T24=SectScVal*/

	Create table  	work.SectVal as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,(b.value-a.Value) as Value,
					case 	when a.DataSeriesID="T53" then "T21"
							when a.DataSeriesID="T54" then "T22"
							when a.DataSeriesID="T55" then "T23"
							when a.DataSeriesID="T58" then "T24"
					end		as DataSeriesID						
    from 	     	work.IntraSect a
	inner join		work.AnnVP b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*  Calculating PrimaryPlusSecondaryDeflator | PrimaryPlusSecondaryProduction, ConstantDollarProduction | 
	PrimaryPlusSecondaryProduction/ConstantDollarProduction*100 */

	Create table  	work.PrimaryPlusSecondaryDeflator as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryPlusSecondaryDeflator" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.PrimaryPlusSecondaryProduction a
	inner join		(Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(value) as Value 
					from 		work.ConstantDollarProduction b 
					where 		DataSeriesID ^=	"T33" 
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*  Deflating IntraSectoralShipments with PrimaryPlusSecondaryDeflator | IntraSect, PrimaryPlusSecondaryDeflator | 
	IntraSect/PrimaryPlusSecondaryDeflator*100 */

	Create table  	work.ConstantIntraSectoralShipments as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, a.YearNo,(a.Value/b.Value*100) as Value
    from 	     	work.IntraSect a
	inner join		work.PrimaryPlusSecondaryDeflator b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and  (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/* Summing ConstantDollarProduction for entire industry */

	Create table  	work.ConstantDollarTotal as 
	Select 			IndustryID, CensusPeriodID, YearID, YearNo, sum(value) as Value 
	from 			work.ConstantDollarProduction 
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;


/* Indexing ConstantDollarProduction for entire industry to YearNo 1*/

	Create table  	work.ConstantDollarProdIdx as 
    Select          a.IndustryID, a.CensusPeriodID, "ConstantDollarProdIdx" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.ConstantDollarTotal a
	inner join		work.ConstantDollarTotal b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (b.YearNo=1);


/*	Calculating OutAdRat (T90) - Output weighting effect | AnnOut, ConstantDollarProdIdx | 
	AnnOut/ConstantDollarProdIdx */

	Create table  	work.OutAdRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T90" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value) as Value
    from 	     	work.AnnOut a
	inner join		work.ConstantDollarProdIdx b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/* 	Calculating SectoralConstantDollarProduction | ConstantIntraSectoralShipments, ConstantDollarTotal |
	ConstantDollarTotal-ConstantIntraSectoralShipments */

	Create table  	work.SectoralConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, 
					a.YearNo,(b.Value-a.Value) as Value
    from 	     	work.ConstantIntraSectoralShipments a
	inner join		work.ConstantDollarTotal b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Indexing SectoralConstantDollarProduction to Year No 1 */

	Create table  	work.SectoralConstantProductionIndex as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.SectoralConstantDollarProduction a
	inner join		work.SectoralConstantDollarProduction b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and b.YearNo=1;


/* 	Calculating Sectoral Output Indexes | SectoralConstantProductionIndex, OutAdRat |
	SectoralConstantProductionIndex * OutAdRat |
	T11=Sect5dOut, T12=Sect4dOut, T13=Sect3dOut, T14=SectScOut*/

	Create table  	work.SectOut as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,(a.Value*b.Value) as Value,
					case 	when a.DataSeriesID="T53" then "T11"
							when a.DataSeriesID="T54" then "T12"
							when a.DataSeriesID="T55" then "T13"
							when a.DataSeriesID="T58" then "T14"
					end		as DataSeriesID						
    from 	     	work.SectoralConstantProductionIndex a
	inner join		work.OutAdRat b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/* 	Calculating WhrEvDfl (T66) | AllCurrentProductionData, ConstantDollarProduction |
	Sum(AllCurrentProductionData)/Sum(ConstantDollarProduction)*100 */

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


/* 	Calculating WhrEvCon (T47) | WhrEvCur, WhrEvDfl | WhrEvCur/WhrEvDfl*100 */

	Create table  	work.WhrEvCon as 
    Select          a.IndustryID, a.CensusPeriodID, "T47" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.WhrEvCur a
	inner join		work.WhrEvDfl b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

quit;

/* Merging calculated variables together along with source data variables */
proc sql;
	Create table 	work.OutManf8702CalculatedVariables as
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
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutManf8702CalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ManufacturingSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
