/*Chronic Kidney Disease Analysis: IPD*/
/*Date: 2024/9/9*/

dm "log; clear;";


libname NHIRDM "F:\H111261\data"; 
libname NHIRDT "F:\H111261\data\data_H111261-1";


libname PRE "F:\H111261\user\JingZhong\CKD\pre";
libname MID "F:\H111261\user\JingZhong\CKD\mid";

/*
libname NHIRDM "C:\Users\r\Desktop\JingZhong\Simulation_Data\Health-05"; 
libname NHIRDT "C:\Users\r\Desktop\JingZhong\Simulation_Data\Health-02"; 
libname PRE "C:\Users\r\Desktop\JingZhong"; 
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
%macro doIpdest;
	%macro year(yr);
		proc sql;
			create table PRE.H_NHI_IPDTE&yr as
			select FEE_YM, APPL_TYPE, APPL_DATE, CASE_TYPE, SEQ_NO, 
				HOSP_ID, ID, IN_DATE, ICD9CM_1, ICD9CM_2, ICD9CM_3,
				ICD9CM_4, ICD9CM_5, ICD_OP_CODE1, ICD_OP_CODE2, ICD_OP_CODE3,
				ICD_OP_CODE4, ICD_OP_CODE5, FUNC_TYPE, HOS, ID_S,
			case when &yr<=104 and 
			(ICD9CM_1 in &icd9
			%do n=2 %to 5;
			| ICD9CM_&n in &icd9 
			%end;) then 1
			when &yr>104 and 
			(ICD9CM_1 in &icd10
			%do n=2 %to 5;
			| ICD9CM_&n in &icd10
			%end;) then 1
			else 0
			end as ICD_DEF
			from NHIRDT.H_NHI_IPDTE&yr(encoding=any);
		quit;
		proc sort 
			data=PRE.H_NHI_IPDTE&yr nodupkey;
			by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ID ID_S;
		run;

		%macro month(mm);
			proc sql;
				create table PRE.H_NHI_IPDTO&yr&mm as
				select a.*, b.*
				from NHIRDM.H_NHI_IPDTO&yr&mm(keep=FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ORDER_CODE
				where=(ORDER_CODE in &hemodialysis | ORDER_CODE in &pd | ORDER_CODE in &transplant))
				as a inner join PRE.H_NHI_IPDTE&yr as b
				on a.FEE_YM = b.FEE_YM & a.APPL_TYPE = b.APPL_TYPE & a.APPL_DATE = b.APPL_DATE & a.CASE_TYPE = b.CASE_TYPE & a.SEQ_NO = b.SEQ_NO & a.HOSP_ID = b.HOSP_ID;
			quit;
			proc sort 
				data=PRE.H_NHI_IPDTO&yr&mm nodupkey;
				by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ID ORDER_CODE ID_S;
			run;
			proc append
				base=PRE.H_NHI_IPDTO&yr
				data=PRE.H_NHI_IPDTO&yr&mm force;
			run;
			proc datasets
				lib=PRE nolist;
				delete H_NHI_IPDTO&yr&mm;
			run;
			quit;
		%mend;
		%do mth=1 %to 12;
        		%let mth1=%sysfunc(putn(&mth, z2.));
        		%month(&mth1);
    		%end;
		
		/*Deal with ORDER_CODE*/
		
		/*Transplant*/
		%let yr_ad = %eval(&yr + 1911);
		
		%if &yr > 103 %then
			%do;
			proc sql;
				insert into WORK.H_NHI_IPDTO&yr
				select * 
				from PRE.IPD_TRANSPLANT_TEMP
			quit;

			proc datasets
				lib=WORK nolist;
				delete IPD_TRANSPLANT_TEMP;
			run;
			quit; 
		%end;

		proc sql;
			create table MID.IPD_TRANSPLANT&yr as
			select *
			from PRE.H_NHI_IPDTO&yr
			where ORDER_CODE in &transplant and month(input(IN_DATE, 8.)) < 7;
		quit;
		
		/*If IN_DATE is after 7/1, it will be categorized into the following year's cases*/
		proc sql;
			create table WORK.IPD_TRANSPLANT_TEMP as
			select *
			from PRE.H_NHI_IPDTO&yr
			where ORDER_CODE in &transplant and month(input(IN_DATE, 8.)) >= 7;
		quit;



		/*Notice that dialysis cases need to be appended by years*/
		proc sql;
			create table PRE.IPD_HEMODIALYSIS&yr as
			select *
			from PRE.H_NHI_IPDTO&yr
			where ORDER_CODE in &hemodialysis and ID not in (select ID from PRE.IPD_TRANSPLANT&yr);

			create table PRE.IPD_PD&yr as
			select *
			from PRE.H_NHI_IPDTO&yr
			where ORDER_CODE in &pd and ID not in (select ID from PRE.IPD_TRANSPLANT&yr);
	
		quit;
		proc append
			base=PRE.IPD_HEMODIALYSIS
			data=PRE.IPD_HEMODIALYSIS&yr force;
		run;
		proc append
			base=PRE.IPD_PD
			data=PRE.IPD_PD&yr force;
		run;
		proc datasets
			lib=PRE nolist;
			delete IPD_HEMODIALYSIS&yr;
		run;
		quit;
		proc datasets
			lib=PRE nolist;
			delete IPD_PD&yr;
		run;
		quit;
		
	%mend;
	%do year=103 %to 103;
		%year(&year);
	%end;

	/*Screen dialysis patients with definition*/
	proc sort data=PRE.IPD_HEMODIALYSIS;
		by ID IN_DATE;
		run;

	data MID.IPD_HEMODIALYSIS_SCREENED;
    		set PRE.IPD_HEMODIALYSIS;
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

	proc sort data=PRE.IPD_PD;
		by ID IN_DATE;
	run;

	data MID.IPD_PD_SCREENED;
    		set PRE.IPD_PD;
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
				insert into PRE.H_NHI_IPDTE&yr
				select * 
				from WORK.IPD_PREESRD_TEMP
			quit;

			proc datasets
				lib=WORK nolist;
				delete IPD_PREESRD_TEMP;
			run;
			quit; 
		%end;

		proc sql;
    			create table MID.IPD_PREESRD&yr as
    			select *
				case when 
				ORDER_CODE in &epo then 1
				else 0
				end as EPO
    			from PRE.H_NHI_IPDTE&yr
    			where ICD_DEF=1 
 			and ID not in (
        		select ID 
        		from PRE.IPD_TRANSPLANT&yr
        		union
        		select ID 
        		from PRE.IPD_HEMODIALYSIS_SCREENED
        		where year(input(IN_DATE, 8.))=&yr_ad and month(input(IN_DATE, 8.)) < 7
        		union
        		select ID 
        		from PRE.IPD_PD_SCREENED
			where year(input(IN_DATE, 8.))=&yr_ad) and month(input(IN_DATE, 8.)) < 7;
		quit;
		proc sort 
			data=MID.IPD_PREESRD&yr nodupkey;
			by FEE_YM APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO HOSP_ID ID ID_S;
		run;

		/*If IN_DATE is after 7/1, it will be categorized into the following year's cases*/
		proc sql;
			create table WORK.IPD_PREESRD_TEMP as
			select *
			from PRE.H_NHI_IPDTE&yr
			where ICD_DEF=1 
 			and ID not in (
        		select ID 
        		from PRE.IPD_TRANSPLANT&yr
        		union
        		select ID 
        		from PRE.IPD_HEMODIALYSIS_SCREENED
        		where year(input(IN_DATE, 8.))=&yr_ad and month(input(IN_DATE, 8.)) >= 7
        		union
        		select ID 
        		from PRE.IPD_PD_SCREENED
			where year(input(IN_DATE, 8.))=&yr_ad) and month(input(IN_DATE, 8.)) >= 7;
		quit;


	%mend;
	%do year=103 %to 103;
		%year_preesrd(&year);
	%end;

%mend;
%doIpdest;



