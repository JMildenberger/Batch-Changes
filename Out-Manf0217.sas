libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for manufacturing. */

data work.ManufacturingSource;
	set LPAll.LP_Append;
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
	ManfOut0217 configuration */

Proc sql;
	Create table	work.OutManf0217 as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-Manf0217.sas";   
quit;


/*	The Year Number is extracted from the variable YearID	*/
data work.IPS_SourceData;
	set work.OutManf0217;
	YearNo=input(substr(YearID,5,1),1.);
run;


/*	Nonemployer Ratio | XT39=VSIndAnn, XT49=VSNonEmp |	(XT39+XT49)/XT39 */

Proc sql;
	Create table	work.SourceNonEmployerRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "NonEmpRatio" as Dataseries, a.YearID, a.YearNo, 
					(a.Value+b.Value)/a.Value as Value
	from 			work.IPS_SourceData a 
	inner join 		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo) and(a.DataSeriesID="XT39") and 
					(b.DataSeriesID="XT49");
quit;


/*Value of Employer Shipments minus Miscellaneous Receipts | XT39=VSIndAnn, XT48=VSMisc | (XT39-XT48) */

Proc sql;
	Create table	work.SourceEmployerShipmentsMinusMisc as 
	Select			a.IndustryID, a.CensusPeriodID, "SourceEmployerShipmentMinusMisc" as Dataseries, 
					a.YearID, a.YearNo, a.Value-b.Value as Value
	from 			work.IPS_SourceData a
	inner join		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo)and (a.DataSeriesID="XT39") and 
					(b.DataSeriesID="XT48");


/*	Ratio of Employer Product Shipments to Total Shipments | SourceEmployerShipmentsMinusMisc, XT39=VSIndAnn |
	(SourceEmployerShipmentsMinusMisc/XT39) */

	Create table	work.ProdShipRatio as 
	Select			a.IndustryID, a.CensusPeriodID, "ProdShipRatio" as Dataseries, a.YearID, a.YearNo, 
					b.Value/a.Value as Value
	from 			work.IPS_SourceData a
	inner join		work.SourceEmployerShipmentsMinusMisc b
	on				(a.DataSeriesID="XT39") and (a.IndustryID=b.IndustryID) and 
					(a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and (a.YearNo=b.YearNo);

	
/*	Ratio of Primary Product Shipments to Total Product Shipments in Census Years | XT42=VSPProd, XT44=VSSProd |
	XT42/(XT42+XT44) */

	Create table	work.PrimProdCensusRatio as 
    Select			a.IndustryID, a.CensusPeriodID, "PrimProdRatioCensus" as Dataseries, a.YearID, a.YearNo,  
					a.Value/(a.Value + b.Value) as Value
    from        	work.IPS_SourceData a
	inner join      work.IPS_SourceData b 
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo) and(a.DataSeriesID="XT42") and (b.DataSeriesID="XT44");


/* 	The "PrimProdRatio" must be interpolated for non-Census years. The next 3 queries calculate the incremental value for the 
	interpolation and adds the inceremental value to the previous years value. If year 6 is not available dataset is empty 
	resulting in the "PrimProdAnnualRatio" table having values set equal to Year 1 */

	Create table  	work.PrimProdCensusRatioDiff as 
    Select          a.IndustryID, a.CensusPeriodID, (a.Value-b.Value)/5 as IncrementValue
    from 	     	work.PrimProdCensusRatio a
	inner join		work.PrimProdCensusRatio b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearNo=6) and (b.YearNo=1);

	Create table	work.PrimProdAnnualRatioWorking as
	Select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, b.Value, 
					case 	when c.IncrementValue is null then 0 
							else c.IncrementValue 
					end 	as IncrementValue
	from			work.SourceEmployerShipmentsMinusMisc a 
	left join 		work.PrimProdCensusRatio b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) 
	left join 		work.PrimProdCensusRatioDiff c
	on 				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);

	Create table	work.PrimProdAnnualRatio as
	Select			a.IndustryID, a.CensusPeriodID, "PrimProdRatioAnnual" as Dataseries, a.YearID, a.YearNo,
					(a.IncrementValue*(a.YearNo-1))+b.Value as Value
	from			work.PrimProdAnnualRatioWorking a
	inner join		work.PrimProdAnnualRatioWorking b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1);


/*Ratio of Resales to Miscellaneous Receipts | XT43=VSResale, XT48=VSMisc | XT43/XT48 */

	Create table  	work.ResalesRatio as 
    Select          a.IndustryID, a.CensusPeriodID, "ResalesRatio" as Dataseries, a.YearID, a.YearNo, 
					case 	when a.value=b.value then 1
							else (a.Value/b.Value) 
					end		as Value
    from 	     	work.IPS_SourceData a
	inner join		work.IPS_SourceData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID="XT43") and (b.DataSeriesID="XT48");


/*Calculating ValShip (T40) | SourceNonEmployerRatio, XT39=VSIndAnn | SourceNonEmployerRatio*XT39 */

	Create table  	work.ValShip as 
    Select          a.IndustryID, a.CensusPeriodID, "T40" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.SourceNonEmployerRatio b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID ="XT39");


/*	Calculating ValShipP (T41) | SourceEmployerShipmentsMinusMisc, SourceNonEmployerRatio, PrimProdAnnualRatio |
   	SourceEmployerShipmentsMinusMisc*SourceNonEmployerRatio*PrimProdAnnualRatio */

	Create table  	work.ValShipP as 
    Select          a.IndustryID, a.CensusPeriodID, "T41" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value*c.Value) as Value
    from 	     	work.SourceEmployerShipmentsMinusMisc a
	inner join		work.SourceNonEmployerRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join		work.PrimProdAnnualRatio c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and 
					(a.YearNo=c.YearNo);


/*	Calculating ValShipS (T42) | SourceEmployerShipmentsMinusMisc, SourceNonEmployerRatio, ValShipP |
   	(SourceEmployerShipmentsMinusMisc*SourceNonEmployerRatio)-ValShipP */

	Create table  	work.ValShipS as 
    Select          a.IndustryID, a.CensusPeriodID, "T42" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value*b.Value)-c.Value as Value
    from 	     	work.SourceEmployerShipmentsMinusMisc a
	inner join		work.SourceNonEmployerRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join		work.ValShipP c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and 
					(a.YearNo=c.YearNo);


/*	Calculating ValShipM (T43) | ValShip, ValShipP, ValShipS | ValShip-ValShipP-ValShipS */

	Create table  	work.ValShipM as 
    Select          a.IndustryID, a.CensusPeriodID, "T43" as DataSeriesID, a.YearID, a.YearNo, 
					(a.Value-b.Value-c.Value) as Value
    from 	     	work.ValShip a
	inner join		work.ValShipP b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join	 	work.ValShipS c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and (a.YearID=c.YearID) and
					(a.YearNo=c.YearNo);


/*	Calculating IntraInd (T52) | SourceNonEmployerRatio, XT41=VSIntra | SourceNonEmployerRatio*XT41 */

	Create table  	work.IntraInd as 
    Select          a.IndustryID, a.CensusPeriodID, "T52" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value*b.Value) as Value
    from 	     	work.IPS_SourceData a
	inner join		work.SourceNonEmployerRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID = "XT41");


/*	Calculating PrimaryIntraIndustryShipments | IntraInd, PrimProdAnnualRatio | IntraInd*PrimProdAnnualRatio */

	Create table  	work.PrimaryIntraIndustryShipments as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryIntraIndustryShipments" as Dataseries, a.YearID, a.YearNo,
					(a.Value*b.Value) as Value
    from 	     	work.IntraInd a
	inner join		work.PrimProdAnnualRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating SecondaryyIntraIndustryShipments | IntraInd, PrimaryIntraIndustryShipments | 
	IntraInd-PrimaryIntraIndustryShipments */

	Create table  	work.SecondaryIntraIndustryShipments as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryIntraIndustryShipments" as Dataseries, a.YearID, a.YearNo,
					(a.Value-b.Value) as Value
    from 	     	work.IntraInd a
	inner join		work.PrimaryIntraIndustryShipments b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating InvBegYr | XT15=InvBOYFG, XT17=InvBOYWP, SourceNonEmployerRatio | (XT15+XT17)*SourceNonEmployerRatio */

	Create table  	work.InvBegYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,
					case when b.method = "NoInvFG" then (0 + d.Value) * a.Value
						 when b.method = "NoInv" then (0 + 0) * a.Value
						else (c.Value + d.Value) * a.Value 
					end as Value
    from		  	work.SourceNonEmployerRatio a
	left join		work.ConfigDistinct b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)
 	left join 		work.IPS_SourceData c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and 
					(a.YearNo=c.YearNo) and (c.DataSeriesID = "XT15")
	left join		work.IPS_SourceData d
    on	 			(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID) and (a.YearID=d.YearID) and 
					(a.YearNo=d.YearNo) and (d.DataSeriesID = "XT17");


/*	Calculating InvEndYr | XT18=InvEOYFG, XT20=InvEOYWP, SourceNonEmployerRatio | (XT18+XT20)*SourceNonEmployerRatio */

	Create table  	work.InvEndYr as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,
					case when b.method = "NoInvFG" then (0 + d.Value) * a.Value
						 when b.method = "NoInv" then (0 + 0) * a.Value
						else (c.Value + d.Value) * a.Value 
					end as Value
    from		  	work.SourceNonEmployerRatio a
	left join		work.ConfigDistinct b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)
 	left join 		work.IPS_SourceData c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and 
					(a.YearNo=c.YearNo) and (c.DataSeriesID = "XT18")
	left join		work.IPS_SourceData d
    on	 			(a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID) and (a.YearID=d.YearID) and 
					(a.YearNo=d.YearNo) and (d.DataSeriesID = "XT20");


/*	Calculating InvChg (T50) | InvEndYr, InvBegYr | InvEndYr-InvBegYr */

	Create table  	work.InvChg as 
    Select          a.IndustryID, a.CensusPeriodID, "T50" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value-b.Value) as Value
    from 	     	work.InvEndYr a
	inner join		work.InvBegYr b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating PrimaryChangeInInventories | InvChg, PrimProdAnnualRatio | InvChg*PrimProdAnnualRatio */

	Create table  	work.PrimaryChangeInInventories as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryChangeInInventories" as Dataseries, a.YearID, a.YearNo,
					(a.Value*b.Value) as Value
    from 	     	work.InvChg a
	inner join		work.PrimProdAnnualRatio b
    on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating SecondaryChangeInInventories | InvChg, PrimaryChangeInInventories | InvChg-PrimaryChangeInInventories */

	Create table  	work.SecondaryChangeInInventories as 
    Select          a.IndustryID, a.CensusPeriodID, "SecondaryChangeInInventories" as Dataseries, a.YearID, a.YearNo,
					(a.Value-b.Value) as Value
    from 	     	work.InvChg a
	inner join		work.PrimaryChangeInInventories b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating Resales (T51) | ValShipM, ResalesRatio | ValShipM*ResalesRatio */

	Create table  	work.Resales as 
    Select          a.IndustryID, a.CensusPeriodID, "T51" as DataSeriesID, a.YearID, a.YearNo,(a.Value*b.Value) as Value
    from 	     	work.ValShipM a
	inner join		work.ResalesRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);


/*	Calculating ValProdM (T33) | ValShipM, Resales | ValShipM-Resales |
	The variable DeflMatch is used to match production values with proper deflators (XT45=DeflMisc) */

	Create table  	work.ValProdM as 
    Select          a.IndustryID, a.CensusPeriodID, "T33" as DataSeriesID, "01" as DataArrayID, 
					a.YearID, a.YearNo,(a.Value-b.Value) as Value, "XT45" as DeflMatch
    from 	     	work.ValShipM a
	inner join		work.Resales b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating ValProdP (T31) | ValShipP, PrimaryIntraIndustryShipments, PrimaryChangeInInventories | 
	ValShipP-PrimaryIntraIndustryShipments+PrimaryChangeInInventories */

	Create table  	work.ValProdP as 
    Select          a.IndustryID, a.CensusPeriodID, "T31" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value-b.Value+c.value) as Value
    from 	     	work.ValShipP a
	inner join		work.PrimaryIntraIndustryShipments b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join 		work.PrimaryChangeInInventories c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID) and (a.YearID=c.YearID) and
					(a.YearNo=c.YearNo);


/*	Calculating ValProdS (T32) | ValShipS, SecondaryIntraIndustryShipments, SecondaryChangeInInventories | 
	ValShipS-SecondaryIntraIndustryShipments+SecondaryChangeInInventories |
	The variable DeflMatch is used to match production values with proper deflators (XT46=DeflSecd) */

	Create table  	work.ValProdS as 
    Select          a.IndustryID, a.CensusPeriodID,  "T32" as DataSeriesID, "01" as DataArrayID, a.YearID, 
					a.YearNo,(a.Value-b.Value+c.value) as Value, "XT46" as DeflMatch
    from 	     	work.ValShipS a
	inner join		work.SecondaryIntraIndustryShipments b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join		work.SecondaryChangeInInventories c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and (a.YearID=c.YearID) and
					(a.YearNo=c.YearNo);


/*	Calculating ValProdP plus ValProdS | ValProdP, ValProdS | ValProdP+ValProdS */

	Create table  	work.PrimaryPlusSecondaryProduction as 
    Select          a.IndustryID, a.CensusPeriodID, "PrimaryPlusSecondaryProduction" as Dataseries, a.YearID, a.YearNo,
					(a.Value+b.Value) as Value
    from 	     	work.ValProdP a
	inner join		work.ValProdS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating AnnVP (T36) | ValProdP, ValProdS, ValProdM | ValProdP+ValProdS+ValProdM */

	Create table  	work.AnnVP as 
    Select          a.IndustryID, a.CensusPeriodID, "T36" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value+b.Value+c.value) as Value
    from 	     	work.ValProdP a
	inner join		work.ValProdS b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo)
	inner join		work.ValProdM c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and (a.YearID=c.YearID) and
					(a.YearNo=c.YearNo);


/*	Indexing AnnVP to YearNo1 */

	Create table  	work.AnnVPIdx as 
    Select          a.IndustryID, a.CensusPeriodID, "AnnVPIdx" as Dataseries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.AnnVP a
	inner join		work.AnnVP b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (b.YearNo=1);


/*	Calculating VPEmp (T38) | AnnVP, SourceNonEmployerRatio | AnnVP * (1/SourceNonEmployerRatio) */

	Create table  	work.VPEmp as 
    Select          a.IndustryID, a.CensusPeriodID, "T38" as DataSeriesID, a.YearID, a.YearNo,
					(a.Value*(1/b.Value)) as Value
    from 	     	work.AnnVP a
	inner join		work.SourceNonEmployerRatio b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Calculating WhrEvCur (T46) which is the sum of wherever made product shipments | XT32=Sale | Sum(XT32) */

	Create table  	work.WhrEvCur as 
    Select          a.IndustryID, a.CensusPeriodID, "T46" as DataSeriesID, a.YearID, a.YearNo, sum(a.Value) as Value
    from 	     	work.IPS_SourceData a
    where 			a.DataSeriesID="XT32"
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;


/* Calculating PPAdjRat (T92) | ValProdP, WhrEvCur | ValProdP/WhrEvCur */

	Create table  	work.PPAdjRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T92" as DataSeriesID, a.YearID, a.YearNo, 
					a.Value/b.Value as Value
    from 	     	work.ValProdP a
	inner join		WhrEvCur b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and
					(a.YearNo=b.YearNo);


/*	Applying PPAdjRat to Sale Data (XT32) | 
	The variable DeflMatch is used to match production values with proper deflators (XT06=Defl) */

	Create table	work.CurrentPrimaryProductionData as 
	Select			a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, "XT06" as DeflMatch, a.YearID, 
					a.YearNo,(a.Value*b.Value) as Value 
	from 			work.IPS_SourceData a
	inner join		work.PPAdjRat b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and a.DataSeriesID="XT32";

/*	Merging production data together for Torqnvist process */

	Create table	work.AllCurrentProductionData as 
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			CurrentPrimaryProductionData 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			ValProdM 
	union all
	Select 			IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, DeflMatch, Value 
	from 			ValProdS;


/*	Querying deflator data together for Torqnvist process | XT06=Defl, XT45=DeflMisc, XT46=DeflSecd */

	Create table  	work.AllDeflatorData as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID, a.DataArrayID, a.YearID, a.YearNo, a.Value
    from 	     	work.IPS_SourceData a
    where 			(a.DataSeriesID="XT06" or a.DataSeriesID="XT45" or a.DataSeriesID="XT46");


/*	 Rebasing deflator data to Year1 */

	Create table  	work.RebasedDeflators as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataSeriesID,a.DataArrayID, a.YearID, a.YearNo, 
					a.Value/b.value*100 as value
    from 	     	work.AllDeflatorData a
	inner join		work.AllDeflatorData b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataSeriesID=b.DataSeriesID) and (a.DataArrayID=b.DataArrayID) and b.YearNo=1;

	
/*  Calculate implicit primary deflator using Physical Quantity Methodolgoy*/

	Create table	work.PrimaryImPrDef as
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
					from 		work.IPS_SourceData a
					inner join 	work.IPS_SourceData b 
					on			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
								(a.DataArrayID=b.DataArrayID)and (a.DataSeriesID="XT31") and (b.DataSeriesID="XT31") and 
								(b.YearNo=1)) b

	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataArrayID=b.DataArrayID);


/* 	Merge deflator data with implicit price deflator calculations using Physical Quantity Methdology*/

	Create table	work.MergedDeflators as
	Select			* 
	from			work.RebasedDeflators union all
	Select			*
	from			work.PrimaryImPrDef;


/*	Deflating Current Dollar Production | AllCurrentProductionData, RebasedDeflators | 
	AllCurrentProductionData/RebasedDeflators */

	Create table  	work.ConstantDollarProduction as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, a.Value/b.value*100 as value			 
    from 	     	work.AllCurrentProductionData a	
	inner join		work.MergedDeflators b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DeflMatch=b.DataseriesID)and (a.DataArrayID=b.DataArrayID) and(a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs		*/

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


/*	Calculating annual product shares of Current Dollar Production */

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


/*	Calculating implicit price deflator | AnnVPidx, AnnOut | AnnVPidx/AnnOut*100 */

	Create table  	work.ImpPrDef as 
    Select          a.IndustryID, a.CensusPeriodID, "ImPrDef" as DataSeries, a.YearID, a.YearNo,
					(a.Value/b.Value*100) as Value
    from 	     	work.AnnVPidx a
	inner join		work.AnnOut b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);


/* Adjusting IntraSectoral source values for Nonemployer| IntraSectoral Source Value*SourceNonEmployerRatio |
	XT08=IntSect1, XT09=IntSect2, XT10=IntSect3, XT11=IntSect4|
	T53=IntraSect5d, T54=IntraSect4d, T55=IntraSect3d, T58=IntraSectSc */

	Create table  	work.IntraSect as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo,(a.Value*b.Value) as Value,
					case 	when a.DataSeriesID="XT08" then "T53"
							when a.DataSeriesID="XT09" then "T54"
							when a.DataSeriesID="XT10" then "T55"
							when a.DataSeriesID="XT11" then "T58"
					end		as DataSeriesID	
    from 	     	work.IPS_SourceData a
	inner join 		work.SourceNonEmployerRatio b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo) and (a.DataSeriesID = "XT08" or a.DataSeriesID = "XT09" or 
					a.DataSeriesID = "XT10" or a.DataSeriesID = "XT11");


/*	Removing intrasectoral shipments from AnnVP to calculate sectoral production values | AnnVP - IntraSect |
	T53=IntraSect5d, T54=IntraSect4d, T55=IntraSect3d, T58=IntraSectSc 
	T21=Sect5dVal, T22=Sect4dVal, T23=Sect3dVal, T24=SectScVal */

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
	T11=Sect5dOut, T12=Sect4dOut, T13=Sect3dOut, T14=SectScOut */

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


/* Merging calculated variables together along with source data variables */

	Create table 	work.OutManf0217CalculatedVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdS union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValProdM union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPEmp union all
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
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutManf0217CalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.ManufacturingSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
