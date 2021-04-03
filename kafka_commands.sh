sh kafka-topics.sh --zookeeper zk0-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:2181 --create --topic transactions --if-not-exists --partitions 1 --replication-factor 

sh kafka-console-producer.sh  --broker-list wn0-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:9092 wn1-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:9092 wn2-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:9092  --topic transactions  < /usr/share/usecase/customer_transaction.csv

sh kafka-console-consumer.sh --bootstrap-server  wn0-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:9092  --topic transactions 

val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "wn0-kafka.ztjmhulr5aiedcnkfkeuamfzja.rx.internal.cloudapp.net:9092").option("subscribe", "transactions").option("startingOffsets", "earliest").load()

df.printSchema()

val transdf = df.selectExpr("CAST(value AS STRING)")

transdf.writeStream.format("console").outputMode("append").start().awaitTermination()


val trans_schema = new StructType().add("Transfer_Key",StringType).add("Account_Key",StringType).add("Transaction_Amount",IntegerType).add("Country",StringType).add("Transaction_Date",StringType)

 val trans_DF = transdf.select(from_json(col("value"), trans_schema).as("data")).select("data.*")


trans_DF.writeStream.format("console").outputMode("append").start().awaitTermination()
