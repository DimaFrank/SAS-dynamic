%let date = %sysfunc(MOD(&validity_date.,1000000));
%let date = %sysfunc(putn(&date.,6));

%let year = %sysfunc(substr(&date.,1,2));
%let month = %sysfunc(substr(&date.,3,2));
%let day = %sysfunc(substr(&date.,5,2));

%let v_date = %str(20&year.-&month.-&day.);
%let folder = &path.;

%let newdir = &folder.\&v_date.;

options dlcreatedir;
libname newdir  "&folder.\&v_date." ;


%let file_name = credit_cards_aggregated.xlsx;

%to_excel(CC_CLUSTERED_NEW_VER, "&newdir.\&file_name.");