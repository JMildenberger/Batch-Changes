libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";


/*	This piece of code is extracting the detailed output series used by the program. When this program goes live, this
	piece of code will be changed to pull from the dataset produced by the detailed output program */

data work.LP_Source;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.LP_OutDet as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.LP_Source
	where 			DataSeriesID in ("T11", "T12", "T13", "T14", "T21", "T22", "T23", "T24", "T36", "T37")
	order by 		IndustryID, DataSeriesID, YearID;
quit;

data work.LP_OutDet;
	set work.LP_OutDet;
	YearNo=input(substr(YearID,5,1),1.);
run;


/*	This code code assigns the appropriate output and valprod dataseries based on the DigitID. Will need further clarification on "DigtiID" for the sector/combo industries.
	T11=Sect5dOut, T12=Sect4dOut, T13=Sect3dOut, T21=Sect5dVal, T22=Sect4dVal, T23=Sect3dVal*/
Proc sql;
	Create table	work.AssignAggMap as
	Select			Distinct IndustryID, ArrayCodeIndustryID, DigitID, CensusPeriodID,
					case	when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="5-Digit" then "T11"
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="4-Digit" then "T12"
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="3-Digit" then "T13" 
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID in ("2-Digit", "2-Sector") then "T14"
							else "T37"
					end as	OutDataSeriesID,
					case	when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="5-Digit" then "T21"
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="4-Digit" then "T22"
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID="3-Digit" then "T23"
							when Program not in ("Out-Rtl.sas", "Out-TQ.sas") and DigitID in ("2-Digit", "2-Sector") then "T24" 
							else "T36"
					end as	ValDataSeriesID
	from			LPAll.AggregateConcordance
	where			IndustrySeriesID="Output"
	order by		IndustryID, CensusPeriodID, ArrayCodeIndustryID;
quit;


Proc sql;
	Create table	work.ConfigDistinct as
	Select 			Distinct IndustryID, IndustrySeriesID, CensusPeriodID, Program, Method
	from 			LPAll.ProgramMethodControlTable
	where 			IndustrySeriesID="Output" and Program="Out-Agg.sas";
quit;


/* The Lab-Agg industries are paired with the aggregation map*/
Proc sql;
	Create table	work.JoinConfigMapwithAggMap as
	Select			a.IndustryID, b.ArrayCodeIndustryID, a.CensusPeriodID, a.Method, b.OutDataSeriesID, b.ValDataSeriesID
	from			work.ConfigDistinct a
	left join		work.AssignAggMap b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID;
quit;


/* The YearIDs for each industry are brought in */
Proc sql;
	Create table	work.JoinYearMap as
	Select			a.IndustryID, a.ArrayCodeIndustryID, a.CensusPeriodID, a.Method, b.YearID, a.OutDataSeriesID, a.ValDataSeriesID
	from			work.JoinConfigMapwithAggMap a
	left join		LPAll.SAS_IndustrySeriesYears b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.IndustrySeriesID="Output";
quit;

/* This code brings in the detailed sectoral output indexes for each detailed industry in the AssignAggMap dataset */
Proc sql;
	Create table	work.OutDataSet as
	Select			a.IndustryID, a.ArrayCodeIndustryID, b.DataSeriesID, b.YearID, b.CensusPeriodID, b.YearNo, b.Value
	from			work.JoinYearMap a
	inner join		work.LP_OutDet b
	on				(a.ArrayCodeIndustryID=b.IndustryID) and (a.OutDataSeriesID=b.DataSeriesID) and (a.YearID=b.YearID);
quit;

/* This code brings in the detailed sectoral production values for each detailed industry in the AssignAggMap dataset */
Proc sql;
	Create table	work.ValDataSet as
	Select			a.IndustryID, a.ArrayCodeIndustryID, b.DataSeriesID, b.YearID, b.CensusPeriodID, b.YearNo, b.Value
	from			work.JoinYearMap a
	inner join		work.LP_OutDet b
	on				(a.ArrayCodeIndustryID=b.IndustryID) and (a.ValDataSeriesID=b.DataSeriesID) and (a.YearID=b.YearID);
quit;


/*	Calculating logarithmic change in the sectoral output indexes */
Proc sql;
	Create table  	work.LogarithmicChange as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.ArrayCodeIndustryID, a.YearID, a.YearNo,
					log(a.value)-log(b.value) as value
    from 	     	work.OutDataSet a 
	left join 		work.OutDataSet b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID)and (a.ArrayCodeIndustryID=b.ArrayCodeIndustryID) and 
					(a.YearNo-1=b.YearNo);
quit;

/*	Calculating annual product shares of sectoral production */
Proc sql;
	Create table  	work.AnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.ArrayCodeIndustryID, a.YearID, a.YearNo,
					a.value/sum(a.value) as value
    from 	     	work.ValDataSet a 
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;


/*	Calculating average annual shares of sectoral production */
Proc sql;
	Create table  	work.AverageAnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.ArrayCodeIndustryID, a.YearID, a.YearNo,
					(a.value+b.value)/2 as value
    from 	     	work.AnnualShares a 
	left join 		work.AnnualShares b
    on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID) and (a.ArrayCodeIndustryID=b.ArrayCodeIndustryID) and (a.YearNo-1=b.YearNo);
quit;


/*	Calculating exponent of sum of weighted industry growth rates | Exp (Sum(LogarithmicChange*AverageAnnualShares)) */
Proc sql;
	Create table  	work.ExpSum as 
    Select          a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo, exp(sum(a.value*b.value)) as value
    from 	     	work.LogarithmicChange a
	inner join		work.AverageAnnualShares b
    on	 			(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.ArrayCodeIndustryID=b.ArrayCodeIndustryID) and (a.YearID=b.YearID)
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;

/*	Calculating AnnOut (T37) via chain linking */
Proc sql;
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

/* Calculating AnnVP (T36) */
Proc sql;
	Create table  	work.AnnVP as 
    Select          IndustryID, CensusPeriodID, "T36" as DataseriesID, YearID, sum(a.value) as value
    from 	     	work.ValDataSet a 
	group by		a.IndustryID, a.CensusPeriodID, a.YearID, a.YearNo;
quit;

Proc sql;
	Create table 	work.OutAggCalculatedVariables as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodiD, Value 	from work.AnnVP;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OutAggCalculatedVariables union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.LP_Source
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;