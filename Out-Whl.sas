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
	Out-Whl configuration */

Proc sql;
	Create table	work.OutWhl as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-Whl.sas";   
quit;


/*	The Year Number is extracted from the variable YearID	*/

data work.IPS_SourceData;
	set work.OutWhl;
	if Value = . then Value = 0;
	YearNo=input(substr(YearID,5,1),1.);
run;

/*	IndYears creates the list of distinct industry data values*/

proc sql;
	create table 	work.IndYears as
	select			distinct IndustryID, YearID, CensusPeriodID, YearNo
	from			work.IPS_SourceData;
quit;

/*	Interpolation Macros */

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

/*	Merchant Wholesaler Sales Ratio | XT26=MWSales, XT34=SalesMW |	(XT34/1000)/XT26 */

proc sql;
	create table	work.MWSalesRatioCenYears as
	select			a.IndustryID, "MW Sales Ratio Census Years" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, "00" as DataArrayID, 
						Case when a.Value/1000 = b.Value then 1 else (a.Value/1000)/b.Value end as Value
	from			work.IPS_sourcedata a
	inner join		work.IPS_sourcedata b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (B.DataSeriesID="XT26") and (A.DataSeriesID="XT34");
quit;

%InterpolateIndustry (MWSalesRatioCenYears, IndYears);

/*	ValShipMW | XT26=MWSales,  | XT26=MWSales AnnualMWSalesRatioCenYears | XT26*AnnualMWSalesRatioCenYears */

proc sql;
	create table	work.VSMW as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T45" as DataseriesID, "00" as DataArrayID, a.Value*b.Value as Value
	from			work.Annualmwsalesratiocenyears a
	inner join		work.IPS_sourcedata b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (B.DataSeriesID="XT26");
quit;

/*	Intra-Industry Adjustment Ratio | XT29=PerMW |	XT29/100 */

proc sql;
	create table	work.perMWratio as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value/100 as Value
	from			work.IPS_SourceData
	where			DataseriesID="XT29";
quit;

%InterpolateIndustry (perMWratio, IndYears);

/*	Merchant Wholesaler Intra-Industry Sales | Annualpermwratio VSMW |	Annualpermwratio/VSMW */

proc sql;
	create table	work.MWIntraIndSales as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "MW IntraInd Sales" as DataSeriesLabel, "00" as DataArrayID, a.Value*b.Value as Value
	from			work.annualpermwratio a
	inner join		work.VSMW b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/*	ValProd Merchant Wholesaler | VSMW MWIntraIndSales | VSMW-MWIntraIndSales*/

proc sql;
	create table	work.VPMW as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T35" as DataseriesID, "00" as DataArrayID, a.Value-b.Value as Value
	from			work.VSMW a
	inner join		work.MWIntraIndSales b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/*	MANF Total | XT22=MFGCur | sum(MFGCur)*/

proc sql;
	create table	work.MANFWhereverTotal as
	select			IndustryID, "Wherever Sum" as DataSeriesLabel, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT22"
	group by		IndustryID, DataseriesID, YearID, YearNo, CensusPeriodID;
quit;

/*	MANF Sales Ratio | XT33=SalesMSB IPS_SourceData | XT33/IPS_SourceData */

proc sql;
	create table	work.MANFSalesCenRatio as
	select			a.IndustryID, "MANF Sales Ratio Census Years" as DataSeriesLabel, a.CensusPeriodID, a.YearID, a.YearNo, "00" as DataArrayID,
						Case when a.Value/1000 = b.Value then 1 else (a.Value/1000)/b.Value end as Value
	from			work.IPS_sourcedata a
	inner join		work.MANFWhereverTotal b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (A.DataSeriesID="XT33");
quit;

%InterpolateIndustry (MANFSalesCenRatio, IndYears);

/*	ValShipMSB | Annualmanfsalescenratio MANFWhereverTotal | Annualmanfsalescenratio*MANFWhereverTotal */

proc sql;
	create table	work.VSMSBO as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T44" as DataSeriesID, "00" as DataArrayID, a.Value*b.Value as Value
	from			work.annualmanfsalescenratio a
	inner join		work.MANFWhereverTotal b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/*	PerMSB | XT28=PerMSB | XT28/100 */

proc sql;
	create table	work.perMSBratio as
	select			IndustryID, DataSeriesID, YearID, YearNo, CensusPeriodID, DataArrayID, Value/100 as Value
	from			work.IPS_SourceData
	where			DataseriesID="XT28";
quit;

%InterpolateIndustry (perMSBratio, IndYears);

/*	MSBIntraIndSales | Annualpermsbratio T44=VSMSBO | Annualpermsbratio*T44 */

proc sql;
	create table	work.MSBIntraIndSales as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "MSB IntraInd Sales" as DataSeriesLabel, "00" as DataArrayID, a.Value*b.Value as Value
	from			work.annualpermsbratio a
	inner join		work.VSMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/*	ValProdMSB | T44=VSMSBO MSBIntraIndSales | T44-MSBIntraIndSales */

proc sql;
	create table	work.VPMSBO as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T34" as DataseriesID, "00" as DataArrayID, a.Value-b.Value as Value
	from			work.VSMSBO a
	inner join		work.MSBIntraIndSales b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/*	ValShip | T45=VSMW T40=VSMSBO | T45+T44 */

proc sql;
	create table	work.ValShip as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T40" as DataseriesID, "00" as DataArrayID, 
					case 	when  c.Method="NoManf" then a.Value
							when c.Method="Standard" then a.Value+b.Value
					end as 	Value
	from			work.VSMW a
	left join		work.VSMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.ConfigDistinct c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);
quit;

/*	ValShip | T45=VSMW T40=VSMSBO | T45+T44 */

proc sql;
	create table	work.IntraInd as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T52" as DataseriesID, "00" as DataArrayID, 
					case 	when  c.Method="NoManf" then a.Value
							when c.Method="Standard" then a.Value+b.Value
					end as Value
	from			work.MWIntraIndSales a
	left join		work.MSBIntraIndSales b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.ConfigDistinct c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);
quit;

/*	AnnVP | T35=VPMW T40=VSMSBO | T35+T40 */

proc sql;
	create table	work.AnnVP as
	select			a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, "T36" as DataseriesID, "00" as DataArrayID,
					case 	when  c.Method="NoManf" then a.Value
							when c.Method="Standard" then a.Value+b.Value
					end as Value
	from			work.VPMW a
	left join		work.VPMSBO b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID)
	left join		work.ConfigDistinct c
	on				(a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID);
quit;

/*	DeflMW |XT25=MWDefl | XT25/XT25+1 */

proc sql;
	create table	work.DeflMW as
	select			a.IndustryID, a.CensusPeriodID, "T65" as DataSeriesID, a.YearID, a.YearNo, "T35" as ProdMatch, "00" as DataArrayID, a.Value/b.Value*100 as Value
	from			work.IPS_SourceData a
	inner join		work.IPS_SourceData b
	on				(a.IndustryID=b.IndustryID) and (a.DataSeriesID=b.DataSeriesID) and (a.CensusPeriodID=b.CensusPeriodID) and (b.YearNo=1)
	where			a.DataSeriesID="XT25"
	order by 		a.IndustryID, a.DataSeriesID, a.DataArrayID, a.YearID ;			
quit;

/*	MANFWhereverConsTotal | XT21=MfgCst | sum(XT21) */

proc sql;
	create table	work.MANFWhereverConsTotal as
	select			IndustryID, "Wherever Cons Sum" as DataSeriesLabel, YearID, YearNo, CensusPeriodID, sum(Value) as Value
	from			work.IPS_SourceData
	where			DataSeriesID="XT21"
	group by		IndustryID, DataseriesID, YearID, YearNo, CensusPeriodID;
quit;

/*	DeflMSB | MANFWhereverTotal MANFWhereverConsTotal | MANFWhereverTotal/(MANFWhereverConsTotal*100)  */

proc sql;
	create table	work.DeflMSBO as
	select			a.IndustryID, a.CensusPeriodID, "T64" as DataSeriesID, a.YearID, a.YearNo, "T34" as ProdMatch, "00" as DataArrayID, a.Value/b.Value*100 as Value
	from			work.MANFWhereverTotal a
	inner join		work.MANFWhereverConsTotal b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID);
quit;

/* Merging calculated variables VPMW and VPMSBO together */

Proc sql;
	Create table	work.CurrentDollarProduction as
	Select			IndustryID, CensusPeriodID, YearID, YearNo, DataseriesID, DataArrayID, Value
	from			work.VPMW union all
	Select			IndustryID, CensusPeriodID, YearID, YearNo, DataseriesID, DataArrayID, Value
	from			work.VPMSBO;
quit;

/* Merging calculated deflators DeflVPMW and DeflVPMSBO together */

Proc sql;
	Create table	work.AllDeflators as
	Select			IndustryID, CensusPeriodID, YearID, YearNo, ProdMatch, DataArrayID, Value
	from			work.DeflMW union all
	Select			IndustryID, CensusPeriodID, YearID, YearNo, ProdMatch, DataArrayID, Value
	from			work.DeflMSBO;
quit;

/*	ConstDollarProd | CurrentDollarProduction AllDeflators | CurrentDollarProduction/(AllDeflators*100)  */

proc sql;
	create table	work.ConstDollarProd as
	select			a.IndustryID, a.DataSeriesID, "Const Dollar Prod" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, (a.Value/b.Value)*100 as Value
	from			work.CurrentDollarProduction a
	inner join 		work.AllDeflators b
	on				(a.IndustryID=b.IndustryID) and (a.YearID=b.YearID) and (a.DataSeriesID=b.ProdMatch);
quit;


/*	Substitue 0.001 for ConstantDollarProduction values equal to 0. 
	NOTE: This is necessary only for logarithmic change calculation. There is precendent for this in Capital and Hosptial programs		*/
proc sql;
	Create table  	work.Sub_ConstDollarProd as 
    Select          IndustryID, CensusPeriodID, DataSeriesID, DataSeries, DataArrayID, YearID, YearNo,					 
					case when value = 0 then 0.001
						 else value
					end as value
    from 	     	work.ConstDollarProd ;


/*	ConstDollarLnDiff | ConstDollarProd | log(ConstDollarProd) - log(ConstDollarProd) */

	create table	work.ConstDollarLnDiff as
	select			a.IndustryID, a.DataSeriesID, "ConstDollarLnDiff" as DataSeries, a.CensusPeriodID, a.YearID, a.YearNo, a.DataArrayID, log(a.Value)-log(b.Value) as Value
	from			work.Sub_ConstDollarProd a
	left join		work.Sub_ConstDollarProd b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.DataSeriesID=b.DataSeriesID) and (a.YearNo-1=b.YearNo);
quit;

/*	AnnualShares | CurrentDollarProduction | CurrentDollarProduction/sum(CurrentDollarProduction) */

proc sql;
	Create table  work.AnnualShares as 
	Select        a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
				  		a.value/sum(a.value) as value
	from          work.CurrentDollarProduction a 
	group by      a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;

/*	AverageAnnualShares | AnnualShares | Avg(AnnualShares, AnnualShares+1) */

proc sql;
    Create table  work.AverageAnnualShares as 
    Select        a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
				  		(a.value+b.value)/2 as value
    from          work.AnnualShares a 
    left join     work.AnnualShares b
    on                  (a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
                        (a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearNo-1=b.YearNo);
quit;

/*	ExpSum | ConstDollarLnDiff AverageAnnualShares | exp(sum(ConstDollarLnDiff*AverageAnnualShares)) */

proc sql;
    Create table  work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
    from          work.ConstDollarLnDiff a
    inner join    work.AverageAnnualShares b
    on           	(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
                    (a.DataseriesID=b.DataseriesID) and (a.DataArrayID=b.DataArrayID) and (a.YearID=b.YearID) and 
                    (a.YearNo=b.YearNo)
    group by      a.IndustryID, a.CensusPeriodID,  a.YearID, a.YearNo;
quit;

/*	Calculate AnnOut by Chain Linking procedure */

proc sql;
	Create table      work.AnnOut as
	      Select                  a.IndustryID, a.CensusPeriodID, "T37" as DataSeriesID, a.YearID, a.YearNo, 
	                              case when a.YearNo=1 then 100
	                                          when a.YearNo=2 then b.value*100
	                                          when a.YearNo=3 then b.value*c.value*100
	                                          when a.YearNo=4 then b.value*c.value*d.value*100
	                                          when a.YearNo=5 then b.value*c.value*d.value*e.value*100
	                                          when a.YearNo=6 then b.value*c.value*d.value*e.value*f.value*100
	                              end   as Value
	      from              work.ExpSum a 
	      left join         work.ExpSum b
	      on                      (a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID)and b.YearNo=2 
	      left join         work.ExpSum c
	      on                      (a.IndustryID=c.IndustryID) and (a.CensusPeriodID=c.CensusPeriodID)and c.YearNo=3 
	      left join         work.ExpSum d
	      on                      (a.IndustryID=d.IndustryID) and (a.CensusPeriodID=d.CensusPeriodID)and d.YearNo=4 
	      left join         work.ExpSum e
	      on                      (a.IndustryID=e.IndustryID) and (a.CensusPeriodID=e.CensusPeriodID)and e.YearNo=5 
	      left join         work.ExpSum f
	      on                      (a.IndustryID=f.IndustryID) and (a.CensusPeriodID=f.CensusPeriodID)and f.YearNo=6;
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
					from 		work.ConstDollarProd
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) b
	on				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.YearID=b.YearID) and (a.YearNo=b.YearNo);
quit;


/* Calculate Intra-Sectoral Shipments (Constant $) | IntraSect5d, CombPriceDfl |  (IntraSect5d / CombPriceDfl)*100
										 		     IntraSect4d, CombPriceDfl |  (IntraSect4d / CombPriceDfl)*100
											 	     IntraSect3d, CombPriceDfl |  (IntraSect3d / CombPriceDfl)*100
											 	     IntraSectSc, CombPriceDfl |  (IntraSectSc / CombPriceDfl)*100                       	*/
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

/*Calculate Sectoral Production (Constant $) | Sum(ConstDollarProd), IntraSect5d_cons |  Sum(ConstDollarProd) - IntraSect5d_cons
										       Sum(ConstDollarProd), IntraSect4d_cons |  Sum(ConstDollarProd) - IntraSect4d_cons
											   Sum(ConstDollarProd), IntraSect3d_cons |  Sum(ConstDollarProd) - IntraSect3d_cons
											   Sum(ConstDollarProd), IntraSectSc_cons |  Sum(ConstDollarProd) - IntraSectSc_cons 	*/
Proc sql;
	Create table  	work.Sect5d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect5d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstDollarProd	
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect5d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);
				
	Create table  	work.Sect4d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect4d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstDollarProd
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect4d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.Sect3d_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "Sect3d_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstDollarProd
					group by 	IndustryID, CensusPeriodID, YearID, YearNo) a
	inner join		work.IntraSect3d_cons b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and (a.YearID=b.YearID) and 
					(a.YearNo=b.YearNo);

	Create table  	work.SectSc_cons as 
    Select          a.IndustryID, a.CensusPeriodID, "SectSc_cons" as DataSeries, a.YearID, a.YearNo, (a.Value - b.Value) as Value
	from    	    (Select 	IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value 
					from 		work.ConstDollarProd
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


/*Calculate Output Weighting Effect (T90)*/
Proc sql;
	Create table  	work.Sum_ConstDollarProd as 
    Select 			IndustryID, CensusPeriodID, YearID, YearNo, sum(Value) as Value
	from 			work.ConstDollarProd	
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;

	Create table  	work.AggInd_ConstDollarProd as 
    Select          a.IndustryID, a.CensusPeriodID, "AggIndex_ConstDollarProd" as DataSeries, a.YearID, a.YearNo, (a.Value/b.Value*100) as Value
    from 	     	work.Sum_ConstDollarProd a
	inner join		work.Sum_ConstDollarProd b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and
					b.YearNo=1;

	Create table  	work.OutAdRat as 
    Select          a.IndustryID, a.CensusPeriodID, "T90" as DataSeriesId, a.YearID, a.YearNo, (a.Value / b.Value) as Value
    from 	     	work.AnnOut a
	inner join		work.AggInd_ConstDollarProd b
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
									        	          AnnVp, IntraSectSc |  AnnVP - IntraSectSc             */
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
	Create table 	work.OutWhlCalcVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVP union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.ValShip union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraInd union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.IntraSect union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.OutAdRat union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VPMW union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VSMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.VSMW union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.DeflMSBO union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.DeflMW 
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutWhlCalcVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.TradeSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;

proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
