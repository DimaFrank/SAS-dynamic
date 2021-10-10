
 /****************************/
%let report_month=202109;    /* 
/***************************/



Proc Sql noprint;
Connect to teradata As ConDbms
(mode=teradata);
select 
	   month_end, month_start_tera, month_end_tera, start_date, start_risk_model_date, end_date, 
	   start_loans_date, end_loans_date, SD_risk_model, Payoff_Date_start, Payoff_Date_end,
	   mnt, mnt1, mnt2, tool_month, last_tool_month, last_year,  nhz_start_date, nhz_end_date,
	   problem_nhz_start_date, problem_nhz_end_date, cif_start_validity_date, cif_end_validity_date, nhz_month,
	   def_base_month, model_date, mnt_panel, mnt_panel_prev_m, mnt_panel_dec_prev_year, cif_validity_date, 
	   mnt_model_ly, mnt_end_year, mnt_end_year1, emergency_days_delay, yhlm_end_year, yhlm_Q1, yhlm_Q2, yhlm_Q3, yhlm_Q4,
	   yhlm0, mnt_RD, kpi_year, weight_year, start_models_date, model_run_date

into:  month_end, :month_start_tera, :month_end_tera, :start_date, :start_risk_model_date, :end_date, 
	   :start_loans_date, :end_loans_date, :SD_risk_model, :Payoff_Date_start, :Payoff_Date_end,
	   :mnt, :mnt1, :mnt2, :tool_month, :last_tool_month, :last_year,  :nhz_start_date, :nhz_end_date,
	   :problem_nhz_start_date, :problem_nhz_end_date, :cif_start_validity_date, :cif_end_validity_date, :nhz_month,
	   :def_base_month, :model_date, :mnt_panel, :mnt_panel_prev_m, :mnt_panel_dec_prev_year, :cif_validity_date, 
	   :mnt_model_ly, :mnt_end_year, :mnt_end_year1, :emergency_days_delay, :yhlm_end_year, :yhlm_Q1, :yhlm_Q2, :yhlm_Q3, :yhlm_Q4,
	   :yhlm0, :mnt_RD, :kpi_year, :weight_year, :start_models_date, :model_run_date


From Connection to ConDbms
(
select 

	   '0' as yhlm_accts_filter,
	   '0' as yhlm_contrs_filter,

	   Last_Working_Day as month_end,
	   CAST( 1||cast(First_Month_Day as DATE FORMAT 'YYMMDD') AS INTEGER) 				   as month_start_tera,
	   CAST( 1||cast(Last_Month_Day as DATE FORMAT 'YYMMDD') AS INTEGER)  				   as month_end_tera,


		1||cast(OADD_MONTHS(First_Month_Day,-2) as DATE FORMAT 'YYMMDD') 			  	   as start_date,
		1||cast(OADD_MONTHS(First_Month_Day,-5) as DATE FORMAT 'YYMMDD') 				   as start_risk_model_date,
		1||cast(Last_Working_Day as DATE FORMAT 'YYMMDD') 								   as end_date,
		1||cast(First_Working_Day as DATE FORMAT 'YYMMDD') 								   as start_loans_date,
		1||cast(Last_Working_Day as DATE FORMAT 'YYMMDD') 								   as end_loans_date,


	   CAST( 1||cast(OADD_MONTHS(First_Month_Day,-1) as DATE FORMAT 'YYMMDD') AS INTEGER)  as SD_risk_model,
	   CAST( 1||cast(OADD_MONTHS(First_Month_Day,-13) as DATE FORMAT 'YYMMDD') AS INTEGER) as Payoff_Date_start,
	   CAST( 1||cast(OADD_MONTHS(Last_Month_Day,-13) as DATE FORMAT 'YYMMDD') AS INTEGER)  as Payoff_Date_end,


	   Calendar_Month as mnt,
	   

	   case 
			when CAST(substring(CAST(&report_month. AS varchar(6))  from 5 for 6) AS INTEGER) = 1 
				then (Calendar_Month-89)

			else (Calendar_Month-1)

		end as mnt1,


		case 
			when CAST(substring(CAST(&report_month. AS varchar(6))  from 5 for 6) AS INTEGER) = 1 
				then (Calendar_Month-90)

			else (Calendar_Month-2)

		end as mnt2,


	   
		CAST(Calendar_Month as CHAR(6))				as tool_month,
		CAST(mnt1 AS CHAR(6)) 						as last_tool_month,
	    CAST( ((Theyear-1)*100 +12) AS CHAR(6) )    as last_year,

		month_start_tera   							as nhz_start_date,
		month_end_tera     							as nhz_end_date,


	   (MOD(Theyear,1000) + 100) *10000 +101 		as problem_nhz_start_date,
		month_end_tera     					 		as problem_nhz_end_date,

		problem_nhz_start_date 				 		as cif_start_validity_date,
		month_end_tera    					 		as cif_end_validity_date,
		Calendar_Month 	   					 		as nhz_month,   
	

		case 
			when Thequarter in (1,2)
				then ((Theyear-1)*100 +12)

			when Thequarter in (3,4)
				then (Theyear*100 +6)

		else NULL
		end as def_base_month,

		
	    CAST( 1||cast(OADD_MONTHS(First_Month_Day,-1) as DATE FORMAT 'YYMMDD') AS INTEGER) as model_date,

		(&report_month.-1) as mnt_panel,
		(&report_month.-2) as mnt_panel_prev_m,
		CAST( ((Theyear-1)*100 +12) AS INTEGER) as mnt_panel_dec_prev_year,


		CAST( 1||cast(Last_Working_Day as DATE FORMAT 'YYMMDD') AS INTEGER) as cif_validity_date,

		((Theyear-1)*100 +12) as mnt_model_ly,

		((Theyear-1)*100 +12) as mnt_end_year,
 		((Theyear-1)*100 +12) as mnt_end_year1,


		
		(SELECT  
	  			CASE WHEN HebDay NOT IN ('йщйщ' , 'ъбщ') THEN 1
		   			 ELSE 2
	  			END AS emergency_days_delay

			FROM calendar2
			WHERE TheDate = (select Last_Working_Day
							 from calendar_M
						 	 where Calendar_Month = &report_month.)) as emergency_days_delay,


	
		((Theyear-1)*100 +12) as yhlm_end_year,
		((Theyear)*100 +03)   as yhlm_Q1,
		((Theyear)*100 +06)   as yhlm_Q2,
		((Theyear)*100 +09)   as yhlm_Q3,
		((Theyear)*100 +12)   as yhlm_Q4,


		 case 
		 	when Thequarter = 1
			 	then (Theyear-1)*100 +12

			when Thequarter = 2
				then (Theyear*100)+3

			when Thequarter = 3
				then (Theyear*100)+6
			
			when Thequarter = 4
				then (Theyear*100)+10

			else NULL
			end as yhlm0,


		 yhlm0   as mnt_RD,
		

		 Theyear as kpi_year,
		 Theyear as weight_year,

		 ((MOD(Theyear,1000)+100) *10000  +501) as start_models_date,


		 1||cast((select MAX(model_run_date) 
		  from credit_mgmt.risk_models_updates ) as DATE FORMAT 'YYMMDD')  as model_run_date
/*
(select MAX(model_run_date) 
		  from credit_mgmt.risk_models_updates ) as model_run_date
*/



from calendar_M
where Calendar_Month = &report_month.
		
);
DisConnect From ConDbms;
Quit;



%let mnt=&report_month.;
%put &mnt.;
%put &start_models_date.;
		


%let model_db = dwp1_party_view; 
%let mnt_model = %eval(20%substr(&model_date.,2,4));


%let mnt_start_year = %eval(%substr(&mnt.,1,4)01);



%let mnt_model1   = %eval(&mnt_model.-1); 
%let last_yhlm_Q1 = %eval(&yhlm_Q1.-100);
%let last_yhlm_Q2 = %eval(&yhlm_Q2.-100);
%let last_yhlm_Q3 = %eval(&yhlm_Q3.-100);
%let last_yhlm_Q4 = %eval(&yhlm_Q4.-100);


data _null_;
oldvar = "&tool_month";
length newvar $1000;
do i = 1 to countw(oldvar,',');
  newvar = catx(',',newvar,quote(scan(oldvar,i,','),"'"));
end;
call symput('tool_month',trim(newvar));
run;



data _null_;
oldvar = "&last_tool_month";
length newvar $1000;
do i = 1 to countw(oldvar,',');
  newvar = catx(',',newvar,quote(scan(oldvar,i,','),"'"));
end;
call symput('last_tool_month',trim(newvar));
run;




data _null_;
oldvar = "&last_year";
length newvar $1000;
do i = 1 to countw(oldvar,',');
  newvar = catx(',',newvar,quote(scan(oldvar,i,','),"'"));
end;
call symput('last_year',trim(newvar));
run;



%let kpi_year = %trim(&kpi_year.);
%let weight_year = %trim(&weight_year.);

