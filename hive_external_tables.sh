create external table countries_info1(LIST_ID String,ENTITY_ID String) row format delimited fields terminated by ',';
create external table customer_account_info(party_key String,residential_country_cd String) row format delimited fields terminated by ',';
create external table customer_info(account_key String,primary_party_key String) row format delimited fields terminated by ',';
create external table customer_transaction(Transfer_Key String,Account_Key String,Transaction_Amount(in $) int,Transaction Type String,Transaction_Origin/Destination String,Transaction_Date String) row format delimited fields terminated by ',';
