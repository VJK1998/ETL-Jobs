from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql import types as SparkTypes
import uuid
import psycopg2

mongoFormat = "com.mongodb.spark.sql.DefaultSource"
mongoUriOption = "spark.mongodb.input.uri"
mongoCollectionOption = "spark.mongodb.input.collection"

mongoCoreUrl = "mongodb://rpkdbdev:y5MpwjMZv3^z!4eN@rupeek-shard-00-00-knojx.gcp.mongodb.net:27017,rupeek-shard-00-01-knojx.gcp.mongodb.net:27017,rupeek-shard-00-02-knojx.gcp.mongodb.net:27017/rpkdbdev?ssl=true&replicaSet=rupeek-shard-0&authSource=admin"

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
    return pgDF.collect()[0]["max"]


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
            upsertQuery = 'INSERT INTO customer_test (id,mongoid,"createdAt","updatedAt") VALUES (\'{id}\',\'{mongoid}\',\'{createdAt}\', \'{updatedAt}\')' \
                      ' ON CONFLICT (mongoid) DO UPDATE SET "createdAt" = EXCLUDED."createdAt", "updatedAt" = EXCLUDED."updatedAt"'.format(
                        id=row.id, mongoid=row.mongoid, createdAt=row.createdAt, updatedAt=row.updatedAt)
            cursor.execute(upsertQuery)
            conn.commit()
            print(row)
    finally:
        if conn is not None:
            conn.close()


# Run Only After Updating the customer postgres database
def upsertPhones(mongoUserDF):
    phonesToBeUpsertedDF = mongoUserDF.select(sf.col('phone').alias('primaryPhone'), 'createdAt', 'updatedAt', 'phones', 'mongoid', )
    mongoIDs = [row.mongoid for row in phonesToBeUpsertedDF.collect()]
    mongoIDsString = ("' , '").join(mongoIDs)
    pgQuery = "(SELECT id, mongoid FROM customer_test WHERE mongoid IN ('"+mongoIDsString+"')) foo"
    pgCustomerDF = getPGDataframe(pgQuery)
    phonesAssociatedCustomerDF = phonesToBeUpsertedDF.join(pgCustomerDF, on=['mongoid'], how='left')
    # The id in the join is customer's id not postGres Phone id
    phonesAssociatedCustomerDF = phonesAssociatedCustomerDF.select(sf.col('id').alias('customer'), 'updatedAt', 'createdAt', 'primaryPhone', 'phones')
    phones = phonesAssociatedCustomerDF.select('customer', 'createdAt', 'updatedAt', 'primaryPhone', sf.explode_outer('phones').alias('otherPhone'))
    phones = phones.withColumn('otherPhone', sf.when(sf.col('otherPhone').isNull(), sf.col('primaryPhone')).otherwise(sf.col('otherPhone')))
    phones = phones.withColumn('isPrimary', sf.when(sf.col('otherPhone') == sf.col('primaryPhone'), True).otherwise(False))
    phones = phones.select('customer', 'createdat', 'updatedat', sf.col('otherPhone').alias('phone'), 'isPrimary')
    pgPhonesDF = getPGDataframe("customerphones_test")
    phones = phones.filter(~(sf.concat(sf.col('customer'), sf.lit('_'), sf.col('phone')))
                           .isin([str(row.customer)+'_'+str(row.phone) for row in pgPhonesDF.collect()]))
    writeDaataframetoPG(phones, "customerphones_test")


def getMongoCustomerProfileDataframe(CPrecentUpdatedAt):
    mongoCollection = "customerprofile"
    pipeline = [{'$match': {"updatedAt": {'$gt': {'$date': CPrecentUpdatedAt}}}}]
    mongoCustomerProfileDF = spark.read.format(mongoFormat)\
        .option(mongoUriOption, mongoCoreUrl)\
        .option(mongoCollectionOption,mongoCollection)\
        .option("pipeline", pipeline)\
        .load()
    mongoCustomerProfileDF = mongoCustomerProfileDF.select(sf.col('_id.oid').alias('lenderid'),'user', 'lpinfo', sf.col('createdat').alias('createdAtLender'), sf.col('updatedAt').alias('updatedAtLender'))
    mongoCustomerProfileDF.printSchema()
    return mongoCustomerProfileDF

def writeDaataframetoPG(df, pgDB):
    df.write.format('jdbc').mode('append').option('url', pgConnUrl).option('user', pgUser)\
        .option('password', pgPassword).option('dbtable', pgDB).option("driver", "org.postgresql.Driver").save()

def upsertLenderCustomer():
    lenderRecentUpdatedAt = getMostRecentUpdatedAt('lendercustomer_test')

    mongoCustomerProfileDF = getMongoCustomerProfileDataframe(lenderRecentUpdatedAt)

    query = '(SELECT id,mongoid FROM customer_test) foo'
    pgCustomerDF = getPGDataframe(query)
    lenderCustomerDF = mongoCustomerProfileDF.join(pgCustomerDF, pgCustomerDF.mongoid == mongoCustomerProfileDF.user, how='left')
    lenderCustomerDF = lenderCustomerDF.select(sf.col('id').alias('customer'), 'createdat', 'updatedAt',
                                               sf.explode('lpinfo').alias('lpinfo'))
    # lenderCustomerDF = lenderCustomerDF.filter(~sf.col('customer').isNull())
    lenderCustomerDF = lenderCustomerDF\
        .withColumn('rupeek_ref', lenderCustomerDF.lpinfo.rupeekid)\
        .withColumn('lender', lenderCustomerDF.lpinfo.name)\
        .withColumn('lender_ref', lenderCustomerDF.lpinfo.id)

    lenderCustomerDF = lenderCustomerDF.select('customer', 'createdat', 'updatedat', 'rupeek_ref', 'lender', 'lender_ref')
    lenderCustomerDF = attachIdToDataframe(lenderCustomerDF)
    lenderCustomerDF.printSchema()

    pgLenderCustomerDF = getPGDataframe('lendercustomer_test')
    # print('------ count -------')
    # lenderCustomerDF.count()
    # print('------ count -------')
    # lenderCustomerDF.show(20, False)
    return lenderCustomerDF


def attachIdToDataframe(dataframe):
    dataframe = dataframe.withColumn('id', uuidUDF())
    return dataframe



def main():
    global spark
    spark = SparkSession.builder.appName("Mongo to Postgres Transformation").getOrCreate()
    customerRecentUpdatedAt = getMostRecentUpdatedAt('customer').isoformat()
    mongoUserDF = getMongoUserDataframe(customerRecentUpdatedAt)
    upsertCustomers(mongoUserDF)
    upsertPhones(mongoUserDF)
    # upsertLenderCustomer()



if __name__ == '__main__':
    main()
