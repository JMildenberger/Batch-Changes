libname LPAll "J:\SAS Testing\Labor Productivity in SAS\LP All Sectors\Libraries\Intermediate";

/*	This query extracts from IPS all source DataSeriesIDs for output(XT) for mining. */
data work.TQSource;
	set LPAll.LP_Append;
run;

Proc sql;
	Create table 	work.OutputSource as 
	select 			Distinct IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value
	from 			work.TQSource
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

/* 	This query uses the configuration concordance to filter only Industry/CensusPeriodIDs that use the OUT-TQ configuration */
Proc sql;
	Create table	work.OutTQ as
	Select			a.*, b.Program, b.Method                                      
	from			work.OutputSource a
	inner join		work.ConfigDistinct b
	on				a.IndustryID=b.IndustryID and a.CensusPeriodID=b.CensusPeriodID
	where			b.Program="Out-TQ.sas";   
quit;

/*	The Year Number is extracted from the variable YearID	*/
data work.IPS_SourceData;
	set work.OutTQ;
	YearNo=input(substr(YearID,5,1),1.);
run;


/**********************************/
/*Annual Value of Production (T36)*/
/**********************************/

/*	Calculating AnnVP (T36) | XT53 = T36 */
proc sql;
	create table	work.AnnVp as
	select 			IndustryID, CensusPeriodID, "T36" as DataSeriesID, YearID, YearNo, Sum(Value) as Value
	from 			work.IPS_SourceData a
	where			DataSeriesID = "XT53"
	group by 		IndustryID, CensusPeriodID, YearID, YearNo;;
quit;


/********************/
/*Output Index (T37)*/
/********************/

/*	Calculating logarithmic change */
proc sql;
	Create table  	work.LogarithmicChange as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					log(a.value)-log(b.value) as value
    from 	     	(select IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, 
						case when value = 0 then 0.001
							else value
						end as Value
					from work.IPS_SourceData a
					where DataSeriesID = "XT52") a
	left join 		(select IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, 
						case when value = 0 then 0.001
							else value
						end as Value
					from work.IPS_SourceData a
					where DataSeriesID = "XT52") b
	on 				(a.IndustryID=b.IndustryID) and (a.CensusPeriodID=b.CensusPeriodID) and 
					(a.DataseriesID=b.DataseriesID)and (a.DataArrayID=b.DataArrayID) and 
					(a.YearNo-1=b.YearNo);
quit;

/*	Calculating annual product shares of Current Dollar Production */
proc sql;
	Create table  	work.AnnualShares as 
    Select          a.IndustryID, a.CensusPeriodID, a.DataseriesID, a.DataArrayID, a.YearID, a.YearNo, 
					a.value/sum(a.value) as value
    from 	     	(select IndustryID, CensusPeriodID, DataSeriesID, DataArrayID, YearID, YearNo, Value
					from work.IPS_SourceData a
					where DataSeriesID = "XT53") a 
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



/*	Setting all sectoral value of production series equal to AnnVP (T36) 
	T21=Sect5dVal, T22=Sect4dVal, T23=Sect3dVal, T24=SectScVal*/
%macro SectVal;
%do i = 21 %to 24;
Proc sql;
	Create table 	work.SectValT&i as
	Select			IndustryID, "T&i" as DataSeriesID, YearID, YearNo, CensusPeriodID, Value
	from 			work.AnnVP;
quit;
%end;

Proc sql;
	Create table 	work.SectVal as
	%do b = 21 %to 23;
	Select			* from work.SectValT&b union all
	%end;
	Select			* from work.SectValT24;
quit;
%mend SectVal;
%SectVal;



/*	Setting all sectoral output series equal to AnnOut (T37) 
T11=Sect5dOut, T12=Sect4dOut, T13=Sect3dOut, T14=SectScOut */
%macro SectOut;
%do i = 11 %to 14;
Proc sql;
	Create table 	work.SectOutT&i as
	Select			IndustryID, "T&i" as DataSeriesID, YearID, YearNo, CensusPeriodID, Value
	from 			work.AnnOut;
quit;
%end;

Proc sql;
	Create table 	work.SectOut as
	%do b = 11 %to 13;
	Select			* from work.SectOutT&b union all
	%end;
	Select			* from work.SectOutT14;
quit;
%mend SectOut;
%SectOut;


/* Merging calculated variables together along with source data variables */
proc sql;
	Create table 	work.OUTTQFinalVars as
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnVp union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.AnnOut union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectVal union all
	Select 			IndustryID, DataSeriesID, "0000" as DataArrayID, YearID, CensusPeriodID, Value 	from work.SectOut
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

	Create table 	LPAll.LP_Append as
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.OUTTQFinalVars union all
	Select			IndustryID, DataSeriesID, DataArrayID, YearID, CensusPeriodID, Value 			from work.TQSource
	order by		IndustryID, DataSeriesID, DataArrayID, YearID;

quit;


proc datasets library=work kill noprint;
run;
quit;

proc catalog c=work.sasmacr kill force;
run;
quit;
