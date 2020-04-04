from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as SparkTypes
import dateutil.parser as parser
import uuid
import psycopg2

mongoFormat = "com.mongodb.spark.sql.DefaultSource"
mongoUriOption = "spark.mongodb.input.uri"
mongoCollectionOption = "spark.mongodb.input.collection"

# mongoCoreUrl = "mongodb://rpkdbdev:y5MpwjMZv3^z!4eN@rupeek-shard-00-00-knojx.gcp.mongodb.net:27017,rupeek-shard-00-01-knojx.gcp.mongodb.net:27017,rupeek-shard-00-02-knojx.gcp.mongodb.net:27017/rpkdbdev?ssl=true&replicaSet=rupeek-shard-0&authSource=admin"
mongoCoreUrl = "mongodb://datasrv:CcMn0kBxZ6gV2pod@prod-shard-00-00-5eaed.mongodb.net:27017,prod-shard-00-01-5eaed.mongodb.net:27017,prod-shard-00-02-5eaed.mongodb.net:27017/rpkdb?authSource=admin&replicaSet=prod-shard-0&readPreference=primary&appname=MongoDB%20Compass%20Community&retryWrites=true&ssl=true"

pgUriOption = "url"

pgConnUrl = "jdbc:postgresql://dataexp.cgmpejvbbeww.ap-south-1.rds.amazonaws.com/customerinfo"
pgUser = "csdbadmin"
pgPassword = "FI0Yd*3HHfetKyUs"
pgDriver = "org.postgresql.Driver"

spark = None

uuidUDF = sf.udf(lambda: str(uuid.uuid4()), SparkTypes.StringType())


def getPGDataframe(pgDbTable):
    pgDF = spark.read.format('jdbc').option("url", pgConnUrl).option("dbtable", pgDbTable).option("user",
                                                                                                  pgUser).option(
        "password", pgPassword).option("driver", pgDriver).load()
    return pgDF



def getMostRecentUpdatedAt(dbtable):
    query = '(SELECT max("updatedAt") FROM '+dbtable+') foo'
    pgDF = spark.read.format('jdbc').option("url", pgConnUrl).option("user", pgUser).option(
        "password", pgPassword).option("driver", pgDriver).option("dbtable", query).load()
    recentDate = pgDF.collect()[0]["max"]
    return recentDate.isoformat() if recentDate is not None else parser.parse('1970-01-01 00:00:00.000+00').isoformat()


def getMongoUserDataframe(customerRecentUpdatedAt):
    mongoCollection = "user"
    pipeline = [{'$match': {"updatedAt": {'$gt': {'$date': customerRecentUpdatedAt}}}}]
    mongoUserDF = spark.read.format(mongoFormat)\
        .option(mongoUriOption, mongoCoreUrl)\
        .option(mongoCollectionOption, mongoCollection)\
        .option("pipeline", pipeline)\
        .load()
    mongoUserDF = mongoUserDF.select(sf.col('_id.oid').alias('mongoid'), 'phone', 'createdAt', 'updatedAt',
                                     'phones')
    return mongoUserDF


def upsertCustomers(mongoUserDF):
    customersToBeUpsertedDF = mongoUserDF.select('mongoid', 'createdAt', 'updatedAt',)
    customersToBeUpsertedDF = attachIdToDataframe(customersToBeUpsertedDF)
    customersToBeUpserted = [row for row in customersToBeUpsertedDF.collect()]
    conn = None
    try:
        conn = psycopg2.connect(
            "postgresql://csdbadmin:FI0Yd*3HHfetKyUs@dataexp.cgmpejvbbeww.ap-south-1.rds.amazonaws.com/customerinfo?")
        cursor = conn.cursor()
        conn.commit()
        for row in customersToBeUpserted:
            upsertQuery = 'INSERT INTO customers_duplicate (id,mongoid,"createdAt","updatedAt") VALUES (\'{id}\',\'{mongoid}\',\'{createdAt}\', \'{updatedAt}\')' \
                      ' ON CONFLICT (mongoid) DO UPDATE SET "createdAt" = EXCLUDED."createdAt", "updatedAt" = EXCLUDED."updatedAt"'.format(
                        id=row.id, mongoid=row.mongoid, createdAt=row.createdAt, updatedAt=row.updatedAt)
            cursor.execute(upsertQuery)
            conn.commit()
    finally:
        if conn is not None:
            conn.close()


# Run Only After Updating the customer postgres database
def upsertPhones(mongoUserDF):
    phonesToBeUpsertedDF = mongoUserDF.select(sf.col('phone').alias('primaryPhone'), 'createdAt', 'updatedAt', 'phones', 'mongoid', )
    mongoIDs = [row.mongoid for row in phonesToBeUpsertedDF.collect()]
    mongoIDsString = ("' , '").join(mongoIDs)
    pgQuery = "(SELECT id, mongoid FROM customers WHERE mongoid IN ('"+mongoIDsString+"')) foo"
    pgCustomerDF = getPGDataframe(pgQuery)
    phonesAssociatedCustomerDF = phonesToBeUpsertedDF.join(pgCustomerDF, on=['mongoid'], how='left')
    # The id in the join is customer's id not postGres Phone id
    phonesAssociatedCustomerDF = phonesAssociatedCustomerDF.select(sf.col('id').alias('customer'), 'updatedAt', 'createdAt', 'primaryPhone', 'phones')
    phones = phonesAssociatedCustomerDF.select('customer', 'createdAt', 'updatedAt', 'primaryPhone', sf.explode_outer('phones').alias('otherPhone'))
    phones = phones.withColumn('otherPhone', sf.when(sf.col('otherPhone').isNull(), sf.col('primaryPhone')).otherwise(sf.col('otherPhone')))
    phones = phones.withColumn('isPrimary', sf.when(sf.col('otherPhone') == sf.col('primaryPhone'), True).otherwise(False))
    phones = phones.select('customer', 'createdat', 'updatedat', sf.col('otherPhone').alias('phone'), 'isPrimary')
    pgPhonesDF = getPGDataframe("customerphones")
    phones = phones.filter(~(sf.concat(sf.col('customer'), sf.lit('_'), sf.col('phone')))
                           .isin([str(row.customer)+'_'+str(row.phone) for row in pgPhonesDF.collect()]))
    writeDataframetoPG(phones, "customerphones")


def getMongoCustomerProfileDataframe(CPrecentUpdatedAt):
    mongoCollection = "customerprofile"
    pipeline = [{'$match': {"updatedAt": {'$gt': {'$date': CPrecentUpdatedAt}}}}]
    mongoCustomerProfileDF = spark.read.format(mongoFormat)\
        .option(mongoUriOption, mongoCoreUrl)\
        .option(mongoCollectionOption, mongoCollection)\
        .option("pipeline", pipeline)\
        .load()
    return mongoCustomerProfileDF

def writeDataframetoPG(df, pgDB):
    df.write.format('jdbc').mode('append').option('url', pgConnUrl).option('user', pgUser)\
        .option('password', pgPassword).option('dbtable', pgDB).option("driver", "org.postgresql.Driver").option("stringtype","unspecified").save()

def upsertLenderCustomer():
    lenderRecentUpdatedAt = getMostRecentUpdatedAt('lender_customers')
    mongoCustomerProfileDF = getMongoCustomerProfileDataframe(lenderRecentUpdatedAt)
    if mongoCustomerProfileDF.collect() is None or len(mongoCustomerProfileDF.collect()) == 0:
        print('----- There is no data to be upserted -----')
        return
    mongoCustomerProfileDF = mongoCustomerProfileDF.select(sf.col('_id.oid').alias('lenderid'),'user', 'lpinfo', 'createdAt', 'updatedAt')
    query = '(SELECT id,mongoid FROM customers) foo'
    pgCustomerDF = getPGDataframe(query)
    pgCustomerDF.printSchema()
    mongoCustomerProfileDF.printSchema()
    lenderCustomerDF = mongoCustomerProfileDF.join(pgCustomerDF, pgCustomerDF.mongoid == mongoCustomerProfileDF.user.oid, how='left')
    lenderCustomerDF = lenderCustomerDF.select(sf.col('id').alias('customer'), 'createdat', 'updatedAt',
                                               sf.explode('lpinfo').alias('lpinfo'))
    lenderCustomerDF = lenderCustomerDF.filter(~sf.col('customer').isNull())
    # lenderCustomerDF.orderBy(sf.desc('customer')).show(20, False)
    lenderCustomerDF = lenderCustomerDF\
        .withColumn('rupeek_ref', lenderCustomerDF.lpinfo.rupeekid)\
        .withColumn('lender', sf.when(sf.col('lpinfo.name').isNull(), None).otherwise(sf.col('lpinfo.name')))\
        .withColumn('lender_ref', lenderCustomerDF.lpinfo.id)

    lenderCustomerDF = lenderCustomerDF.select('customer', 'createdat', 'updatedat', 'rupeek_ref', 'lender', 'lender_ref')
    lenderCustomerDF = attachIdToDataframe(lenderCustomerDF)
    lenderCustomerDF.printSchema()
    print('=========================================')
    lenderCustomerDF.count()
    print('=========================================')
    # requiredCustomers = [row.customers for row in lenderCustomerDF.collect()]
    # requiredCustomersString = "', '".join(requiredCustomers)
    # requiredLenderRefCustomersQuery = "(SELECT customer, lender_ref FROM lender_customer WHERE customer IN ('"+requiredCustomersString+"')) foo"
    # pgLenderRefCustomerDF = getPGDataframe(requiredLenderRefCustomersQuery)
    # lenderCustomerDF = lenderCustomerDF.filter(~(sf.concat(sf.col('customer'), sf.lit('_'), sf.col('lender_ref')))
    #                                            .isin([str(row.customer)+'_'+str(row.lender_ref) for row in pgLenderRefCustomerDF.collect()]))
    # pgLenderCustomerDF = getPGDataframe('lendercustomers')
    # writeDataframetoPG(lenderCustomerDF, "lender_customers")
    lenderCustomersToBeUpserted = [row for row in lenderCustomerDF.collect()]
    conn = None
    try:
        conn = psycopg2.connect(
            "postgresql://csdbadmin:FI0Yd*3HHfetKyUs@dataexp.cgmpejvbbeww.ap-south-1.rds.amazonaws.com/customerinfo?")
        cursor = conn.cursor()
        conn.commit()
        for row in lenderCustomersToBeUpserted:

            upsertQuery = 'INSERT INTO lendercustomers_duplicate (id,customer,lender_ref,rupeek_ref,lender,"createdAt","updatedAt")' \
                          ' VALUES (\'{id}\',\'{customer}\',\'{lender_ref}\',\'{rupeek_ref}\',\'{lender}\',\'{createdAt}\', \'{updatedAt}\')' \
                          ' ON CONFLICT (customer,rupeek_ref) DO UPDATE SET "lender_ref" = EXCLUDED."lender_ref", "lender" = EXCLUDED."lender", ' \
                          ' "createdAt" = EXCLUDED."createdAt", "updatedAt" = EXCLUDED."updatedAt"'.format(
                id=row.id, customer=row.customer, lender_ref=row.lender_ref, lender=row.lender, rupeek_ref=row.rupeek_ref,  createdAt=row.createdAt, updatedAt=row.updatedAt)
            cursor.execute(upsertQuery)
            conn.commit()
    finally:
        if conn is not None:
            conn.close()


def attachIdToDataframe(dataframe):
    dataframe = dataframe.withColumn('id', uuidUDF())
    return dataframe



def main():
    global spark
    spark = SparkSession.builder.appName("Mongo to Postgres Transformation").getOrCreate()
    customerRecentUpdatedAt = getMostRecentUpdatedAt('customers')
    mongoUserDF = getMongoUserDataframe(customerRecentUpdatedAt)
    upsertCustomers(mongoUserDF)
    # upsertPhones(mongoUserDF)
    # upsertLenderCustomer()


if __name__ == '__main__':
    main()
