/*Chronic Kidney Disease Analysis: IPD*/
/*Date: 2024/9/9*/

dm "log; clear;";


libname NHIRDM "F:\H111261\data"; 
libname NHIRDT "F:\H111261\data\data_H111261-1";
libname PRE "F:\H111261\user\JingZhong\CKD\pre";
libname MID "F:\H111261\user\JingZhong\CKD\mid";

/*
libname NHIRDM "C:\Users\r\Desktop\JingZhong\Simulation_Data\Health-04"; 
libname NHIRDT "C:\Users\r\Desktop\JingZhong\Simulation_Data\Health-01"; 
libname PRE "C:\Users\r\Desktop\JingZhong";
libname MID "C:\Users\r\Desktop\JingZhong"; 
*/

/*---------------------------------------------*/
/*Import ICD code definition*/
proc import
    out=icd9Code
    datafile="F:\H111261\user\JingZhong\CKD\ref\icd9.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\icd9.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(icd))) into :icd9 separated by ", "
	from icd9Code;
quit; 
%let icd9=(&icd9);
%put &icd9;

proc import
    out=icd10Code
    datafile="F:\H111261\user\JingZhong\CKD\ref\icd10.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\icd10.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(icd))) into :icd10 separated by ","
	from icd10Code;
quit; 
%let icd10=(&icd10);

proc import
    out=hemodialysisCode
    datafile="F:\H111261\user\JingZhong\CKD\ref\hemodialysis.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\hemodialysis.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(order_code))) into :hemodialysis separated by ","
	from hemodialysisCode;
quit; 
%let hemodialysis=(&hemodialysis);

proc import
    out=pdCode
    datafile="F:\H111261\user\JingZhong\CKD\ref\pd.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\pd.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(order_code))) into :pd separated by ","
	from pdCode;
quit; 
%let pd=(&pd);

proc import
    out=transplantCode
    datafile="F:\H111261\user\JingZhong\CKD\ref\transplant.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\transplant.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(order_code))) into :transplant separated by ","
	from transplantCode;
quit; 
%let transplant=(&transplant);

proc import
    out=epoCode
    datafile="F:\H111261\user\JingZhong\CKD\ref\epoDrug.csv"
    /*datafile="C:\Users\r\Desktop\JingZhong\ref\epoDrug.csv"*/
    dbms=csv replace;
run;
proc sql noprint;
	select quote(compress(cat(order_code))) into :epo separated by ","
	from epoCode;
quit; 
%let epo=(&epo);

proc delete data=ICD9CODE ICD10CODE HEMODIALYSISCODE PDCODE TRANSPLANTCODE EPOCODE;
quit;

/*Screening patients for CKD*/
%macro doOpdest;
	%macro year(yr);
		%macro month(mm);
			%do k=10 %to 30 %by 10;
				proc sql;
					create table PRE.H_NHI_OPDTE&yr&mm._&k as
					select FEE_YM, APPL_TYPE, APPL_DATE, CASE_TYPE, SEQ_NO, 
						HOSP_ID, ID, FUNC_DATE, ICD9CM_1, ICD9CM_2, ICD9CM_3,
						ICD_OP_CODE1, ICD_OP_CODE2, FUNC_TYPE, HOS, ID_S,
					case when &yr<=104 and 
					(ICD9CM_1 in &icd9
					%do n=2 %to 3;
					| ICD9CM_&n in &icd9 
					%end;) then 1
					when &yr>104 and 
					(ICD9CM_1 in &icd10
					%do n=2 %to 3;
					| ICD9CM_&n in &icd10
					%end;) then 1
					else 0
					end as ICD_DEF
					from NHIRDT.H_NHI_OPDTE&yr&mm._&k(encoding=any);
				quit;
				
				
				proc sql;
					create table PRE.H_NHI_OPDTO&yr&mm._&k as
					select *
					from NHIRDM.H_NHI_OPDTO&yr&mm._&k(encoding=any)
					where DRUG_NO in &hemodialysis | DRUG_NO in &pd | DRUG_NO in &transplant;
				quit;
				
				proc append
					base=PRE.H_NHI_OPDTE&yr
					data=PRE.H_NHI_OPDTE&yr&mm._&k force;
				run;

				proc datasets
					lib=PRE nolist;
					delete H_NHI_OPDTE&yr&mm._&k;
				run;
				proc append
					base=PRE.H_NHI_OPDTO&yr
					data=PRE.H_NHI_OPDTO&yr&mm._&k force;
				run;

				proc datasets
					lib=PRE nolist;
					delete H_NHI_OPDTO&yr&mm._&k;
				run;
			%end;

			proc sort 
				data=PRE.H_NHI_OPDTE&yr nodupkey;
				by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ID ID_S;
			run;

			proc sort 
				data=PRE.H_NHI_OPDTO&yr nodupkey;
				by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID DRUG_NO;
			run;
		%mend;
		%do mth=1 %to 12;
        		%let mth1=%sysfunc(putn(&mth, z2.));
        		%month(&mth1);
    		%end;
		

		proc sql;
			create table PRE.H_NHI_OPDTO&yr(rename=(ORDER_CODE=DRUG_NO IN_DATE=FUNC_DATE)) as
			select a.*, b.*
			from PRE.H_NHI_OPDTO&yr(keep=FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID DRUG_NO 
			where=(DRUG_NO in &hemodialysis | DRUG_NO in &pd | DRUG_NO in &transplant) 
			rename=(ORDER_CODE=DRUG_NO IN_DATE=FUNC_DATE))
				as a inner join PRE.H_NHI_OPDTE&yr as b
				on a.FEE_YM = b.FEE_YM & a.APPL_TYPE = b.APPL_TYPE & a.APPL_DATE = b.APPL_DATE & a.CASE_TYPE = b.CASE_TYPE & a.SEQ_NO = b.SEQ_NO & a.HOSP_ID = b.HOSP_ID;
		quit;


		/*Deal with DRUG_NO*/
		
		/*Transplant*/
		%let yr_ad = %eval(&yr + 1911);
		
		%if &yr > 103  %then
			%do;
			proc sql;
				insert into WORK.H_NHI_OPDTO&yr
				select * 
				from PRE.OPD_TRANSPLANT_TEMP
			quit;

			proc datasets
				lib=WORK nolist;
				delete OPD_TRANSPLANT_TEMP;
			run;
			quit; 
		%end;

		proc sql;
			create table MID.OPD_TRANSPLANT&yr as
			select *
			from PRE.H_NHI_OPDTO&yr
			where ORDER_CODE in &transplant and month(input(IN_DATE, 8.)) < 7;
		quit;
		
		/*If IN_DATE is after 7/1, it will be categorized into the following year's cases*/
		proc sql;
			create table WORK.OPD_TRANSPLANT_TEMP as
			select *
			from PRE.H_NHI_OPDTO&yr
			where ORDER_CODE in &transplant and month(input(IN_DATE, 8.)) >= 7;
		quit;



		/*Notice that dialysis cases need to be appended by years*/
		proc sql;
			create table PRE.OPD_HEMODIALYSIS&yr as
			select *
			from PRE.H_NHI_OPDTO&yr
			where ORDER_CODE in &hemodialysis and ID not in (select ID from PRE.OPD_TRANSPLANT&yr);

			create table PRE.OPD_PD&yr as
			select *
			from PRE.H_NHI_OPDTO&yr
			where ORDER_CODE in &pd and ID not in (select ID from PRE.OPD_TRANSPLANT&yr);
	
		quit;
		proc append
			base=PRE.OPD_HEMODIALYSIS
			data=PRE.OPD_HEMODIALYSIS&yr force;
		run;
		proc append
			base=PRE.OPD_PD
			data=PRE.OPD_PD&yr force;
		run;
		proc datasets
			lib=PRE nolist;
			delete OPD_HEMODIALYSIS&yr;
		run;
		quit;
		proc datasets
			lib=PRE nolist;
			delete OPD_PD&yr;
		run;
		quit;
		
	%mend;
	%do year=103 %to 103;
		%year(&year);
	%end;

	/*Screen dialysis patients with definition*/
	proc sort data=PRE.OPD_HEMODIALYSIS;
		by ID IN_DATE;
		run;

	data MID.OPD_HEMODIALYSIS_SCREENED;
    		set PRE.OPD_HEMODIALYSIS;
    		by ID IN_DATE;
    
    		retain FIRST_DATE;
    		if first.ID then FIRST_DATE=IN_DATE;

    		PERIOD_MONTH = intck('month', FIRST_DATE, IN_DATE);

    		retain COUNT 0;
    		if PERIOD_MONTH<=3 then COUNT+1;
    		else COUNT=1;

    		if last.ID then do;
        	if COUNT>=8 then output;
    		end;

    		if last.ID then COUNT=0;
	run;

	proc sort data=PRE.OPD_PD;
		by ID IN_DATE;
	run;

	data MID.OPD_PD_SCREENED;
    		set PRE.OPD_PD;
    		by ID IN_DATE;
    
    		retain FIRST_DATE;
    		if first.ID then FIRST_DATE=IN_DATE;

    		PERIOD_MONTH = intck('month', FIRST_DATE, IN_DATE);

    		retain COUNT 0;
    		if PERIOD_MONTH<=3 then COUNT+1;
    		else COUNT=1;

    		if last.ID then do;
        	if COUNT>=1 then output;
    		end;

    		if last.ID then COUNT=0;
	run;
	
	%macro year_preesrd(yr);
		%let yr_ad = %eval(&yr + 1911);
		%put &yr_ad;

		%if &yr > 103 %then
			%do;
			proc sql;
				insert into PRE.H_NHI_OPDTE&yr
				select * 
				from WORK.OPD_PREESRD_TEMP
			quit;

			proc datasets
				lib=WORK nolist;
				delete OPD_PREESRD_TEMP;
			run;
			quit; 
		%end;

		proc sql;
    			create table MID.OPD_PREESRD&yr as
    			select *
				case when 
				ORDER_CODE in &epo then 1
				else 0
				end as EPO
    			from PRE.H_NHI_OPDTE&yr
    			where ICD_DEF=1 
 				and ID not in (
        		select ID 
        		from PRE.OPD_TRANSPLANT&yr
        		union
        		select ID 
        		from PRE.OPD_HEMODIALYSIS_SCREENED
        		where year(input(IN_DATE, 8.))=&yr_ad and month(input(IN_DATE, 8.)) < 7
        		union
        		select ID 
        		from PRE.OPD_PD_SCREENED
			where year(input(IN_DATE, 8.))=&yr_ad) and month(input(IN_DATE, 8.)) < 7;
		quit;
		proc sort 
			data=MID.OPD_PREESRD&yr nodupkey;
			by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ID ID_S;
		run;

		/*If IN_DATE is after 7/1, it will be categorized into the following year's cases*/
		proc sql;
			create table WORK.OPD_PREESRD_TEMP as
			select *
			from PRE.H_NHI_OPDTE&yr
			where ICD_DEF=1 
 			and ID not in (
        		select ID 
        		from PRE.OPD_TRANSPLANT&yr
        		union
        		select ID 
        		from PRE.OPD_HEMODIALYSIS_SCREENED
        		where year(input(IN_DATE, 8.))=&yr_ad and month(input(IN_DATE, 8.)) >= 7
        		union
        		select ID 
        		from PRE.OPD_PD_SCREENED
			where year(input(IN_DATE, 8.))=&yr_ad) and month(input(IN_DATE, 8.)) >= 7;
		quit;


	%mend;
	%do year=103 %to 103;
		%year_preesrd(&year);
	%end;

%mend;
%doOpdest;



