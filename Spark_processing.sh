  val spark = SparkSession.builder.appName("proj - module").getOrCreate()  
    # Create a Spark Context
    val sc = spark.sparkContext

 

    # Set Log level in Spark
    sc.setLogLevel("WARN")
    
    //cust key,cust name, cust res country
    val custinfoDF=spark.read.format("csv").schema(csvSchema).load("/user/user4/user/customer_info.csv")
    
    //account key, cust key, account open date
    val custaccountInfoDF=spark.read.format("csv").option("header",true).load("/user/user2/user/customer_account_info.csv")

 

    // transfer_key, account_key,transaction_amt,transaction_type,country_code,transfer_due_date
    val transDF=spark.read.format("csv").option("header",true).load("/user/user4/user/customer_transaction.csv")

 

    val countriesDf = spark.read.format("csv").option("header",true).option("header","true").load("/user/user4/user/countries_info.csv")              

 


    //cust name, cust res country, account key, cust_key, account open date
    custinfoAccounDf =  custinfoDF.join(custaccountInfoDF,custinfoDF.customer_key == custaccountInfoDF.customer_key,"inner") \
                            .drop(custaccountInfoDF.customer_key)

 

    val custinfoAccounDf =  custinfoDF.join(custaccountInfoDF,custinfoDF.party_key === custaccountInfoDF.primary_party_key,"inner")    
    val custinfoAccounDf =  custinfoDF.join(custaccountInfoDF,custinfoDF("party_key") == custaccountInfoDF("primary_party_key"),"inner")    

 


    val finalDf = custinfoAccounDf.join(transDF,custinfoAccounDf.account_key == transDF.account_key,"inner")
                            .drop(transDF.account_key)                         
 
    val finalwithCountriesDf = finalDf.join(countriesDf,finalDf.country_code == countriesDf.entity_key,"left")
    

 

    finalwithCountriesDf.cache()
    
    val HRDF = finalwithCountriesDf.filter(entity_key is not null || \
    (sum(transaction_amt) > 1000 && transaction_type = 'INN') || (sum(transaction_amt) > 800 && transaction_type = 'OUT') ")\
    .withColumn("Risk_level",lit("HR"))
                         
    val MedDF = finalDf.filter("(sum(transaction_amt) > 1000 && transaction_type = 'INN') || (sum(transaction_amt) > 800 && transaction_type = 'OUT') ")\
    .withColumn("Risk_level",lit("LR"))    

    val lowDF = finalDf.filter("entity_key is null || \
    (sum(transaction_amt) > 1000 && transaction_type = 'INN') || (sum(transaction_amt) > 800 && transaction_type = 'OUT') ")
    .withColumn("Risk_level",lit("LR"))

     val finalRiskDf = hrDF.union(medDF).union(lowDF).withColumn("Risk_level",lit("LR"))
    
