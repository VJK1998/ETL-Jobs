from pyspark.sql import SparkSession
from pyspark.sql import functions as SparkFunction
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

uuidUDF = SparkFunction.udf(lambda: str(uuid.uuid4()), SparkTypes.StringType())


def getPGDataframe(pgDbTable):
    pgDF = spark.read.format('jdbc').option("url", pgConnUrl).option("dbtable", pgDbTable).option("user",
                                                                                                  pgUser).option(
        "password", pgPassword).option("driver", pgDriver).load()
    return pgDF



def getMostRecentUpdatedAt(pgCustomerDF):
    return pgCustomerDF.agg(SparkFunction.max(pgCustomerDF.updatedAt)).collect()[0]["max(updatedAt)"]


def getMongoUserDataframe():
    mongoCollection = "user"
    mongoUserDF = spark.read.format(mongoFormat).option(mongoUriOption, mongoCoreUrl).option(mongoCollectionOption,
                                                                                             mongoCollection).load()
    mongoUserDF = mongoUserDF.select(SparkFunction.col('_id.oid').alias('mongoid'), 'phone', 'createdAt', 'updatedAt',
                                     'phones')
    mongoUserDF = attachIdToDataframe(dataframe=mongoUserDF)
    return mongoUserDF


def getUpdatedMongoUserDataFrame(mongoUserDF, pgCustomersDF, recentUpdatedAt):
    updatedUserDF = mongoUserDF.filter(mongoUserDF.updatedAt > recentUpdatedAt)
    updatedUserDF = updatedUserDF.select('mongoid', SparkFunction.col('createdAt').alias('createdAtNew'),
                                         SparkFunction.col('updatedAt').alias('updatedAtNew'))
    joinedUpdatedUserDF = updatedUserDF.join(pgCustomersDF, on=['mongoid'], how='left')
    joinedUpdatedUserDF = joinedUpdatedUserDF.select('id', 'mongoid',
                                                     SparkFunction.col('createdAtNew').alias('createdAt'),
                                                     SparkFunction.col('updatedAtNew').alias('updatedAt'))
    joinedUpdatedUserDF = joinedUpdatedUserDF.withColumn('id', SparkFunction.when(SparkFunction.col('id').isNull(), uuidUDF()).otherwise(SparkFunction.col('id')))
    return joinedUpdatedUserDF


def upsertCustomers(customersToBeUpsertedDF):
    customersToBeUpserted = [row for row in customersToBeUpsertedDF.collect()]
    conn = None
    try:
        conn = psycopg2.connect(
            "postgresql://csdbadmin:FI0Yd*3HHfetKyUs@dataexp.cgmpejvbbeww.ap-south-1.rds.amazonaws.com/customerinfo?")
        cursor = conn.cursor()
        conn.commit()
        for row in customersToBeUpserted:
            upsertQuery = "INSERT INTO customer_test (id,mongoid,createdAt,updatedAt) VALUES ('{id}','{mongoid}','{createdAt}', '{updatedAt}')" \
                      " ON CONFLICT (mongoid) DO UPDATE SET mongoid = EXCLUDED.mongoid , createdAt = EXCLUDED.createdAt, updatedAt = EXCLUDED.updatedAt".format(
                        id=row.id, mongoid=row.mongoid, createdAt=row.createdAt, updatedAt=row.updatedAt)
            cursor.execute(upsertQuery)
            conn.commit()
            print(row)
    finally:
        if conn is not None:
            conn.close()


def extractPhoneFromMongoUsers(mongoUserDF, attachId = True):
    phonesDF = mongoUserDF.select(SparkFunction.col('id').alias('customer'), SparkFunction.col('phone').alias('primaryPhone'), 'createdAt',
                                  'updatedAt', SparkFunction.explode_outer('phones').alias('otherPhone'))
    phonesDF = phonesDF.withColumn('otherPhone', SparkFunction.when(SparkFunction.col('otherPhone').isNull(),
                                                                    SparkFunction.col('primaryPhone')).otherwise(
        SparkFunction.col('otherPhone')))
    phonesDF = phonesDF.withColumn('isPrimary', SparkFunction.when(
        SparkFunction.col('otherPhone') == SparkFunction.col('primaryPhone'), True).otherwise(False))
    phonesDF = phonesDF.select('customer', 'createdAt', 'updatedAt',
                               SparkFunction.col('otherPhone').alias('phone'), 'isPrimary')
    if attachId:
        phonesDF = attachIdToDataframe(phonesDF)
    return phonesDF

# Run Only After Updating the customer postgres database
def upsertPhones(mongoUserDF, pgCustomerDF, pgPhonesDF, recentUpdated):
    mongoUserDF.printSchema()
    phonesToBeUpsertedDF = mongoUserDF.select(SparkFunction.col('phone').alias('primaryPhone'), SparkFunction.col('createdAt').alias('createdAtNew'),
                                              SparkFunction.col('updatedAt').alias('updatedAtNew'), 'phones', 'mongoid')
    phonesToBeUpsertedDF = phonesToBeUpsertedDF.filter(SparkFunction.col('updatedAtNew') > recentUpdated)
    # phonesToBeUpsertedDF.show(30, False)
    phonesAssociatedCustomerDF = phonesToBeUpsertedDF.join(pgCustomerDF, on=['mongoid'], how='left')
    # The id in the join is customer's id not postGres Phone id
    phonesAssociatedCustomerDF = phonesAssociatedCustomerDF.select(SparkFunction.col('id').alias('customer'),
                                                                   SparkFunction.col('updatedAtNew').alias('updatedat'),
                                                                   SparkFunction.col('createdAtNew').alias('createdat'),
                                                                   'primaryPhone',
                                                                   'phones')
    phonesAssociatedCustomerDF = phonesAssociatedCustomerDF.select('customer', 'createdAt', 'updatedAt', 'primaryPhone', 'phones')
    # phonesAssociatedCustomerDF = phonesAssociatedCustomerDF.withColumn('id', uuidUDF())
    customerPhoneDF = phonesAssociatedCustomerDF.select('customer')
    customerID = [row.customer for row in customerPhoneDF.collect() if row.customer]
    phones = phonesAssociatedCustomerDF.select('customer', 'createdAt', 'updatedAt', 'primaryPhone', SparkFunction.explode_outer('phones').alias('otherPhone'))
    phones = phones.withColumn('otherPhone', SparkFunction.when(SparkFunction.col('otherPhone').isNull(), SparkFunction.col('primaryPhone')).otherwise(SparkFunction.col('otherPhone')))
    phones = phones.withColumn('isPrimary', SparkFunction.when(SparkFunction.col('otherPhone') == SparkFunction.col('primaryPhone'), True).otherwise(False))
    phones = phones.select('customer', 'createdat', 'updatedat', SparkFunction.col('otherPhone').alias('phone'), 'isPrimary')
    phones.show(phones.count(), False)
    pgPhonesDF.show(pgPhonesDF.count(), False)
    # print(customersInPG)
    # print(phonesInPG)
    phones = phones.filter(~(SparkFunction.concat(SparkFunction.col('customer'), SparkFunction.lit('_'), SparkFunction.col('phone')))
                           .isin(str(row.customer)+'_'+str(row.phone)) for row in pgPhonesDF.collect())
    phones = attachIdToDataframe(phones)
    phones.show(phones.count(), False)
    # Append newly updated phones
    # phones.write.format('jdbc').mode('append').option('url', pgConnUrl).option('user', pgUser)\
    #     .option('password', pgPassword).option('dbtable', 'customerphones_test').option("driver", "org.postgresql.Driver").save()
    # print(('\n\n\n\n\n ---------------------- Customer IDs ----------------- \n\n\n\n\n'))
    # print(customerID)
    # print(('\n\n\n\n\n ---------------------- Customer IDs ----------------- \n\n\n\n\n'))
    # phonesAssociatedCustomerDF.show(50, False)


def getMongoUserDataframe():
    mongoCollection = "customerprofile"
    mongoCustomerProfileDF = spark.read.format(mongoFormat).option(mongoUriOption, mongoCoreUrl).option(mongoCollectionOption,
                                                                                             mongoCollection).load()
    mongoCustomerProfileDF = mongoCustomerProfileDF.select(SparkFunction.col('_id.oid').alias('lenderid'),'user', 'lpinfo', SparkFunction.col('createdat').alias('createdAtLender'), SparkFunction.col('updatedAt').alias('updatedAtLender'))
    mongoCustomerProfileDF.printSchema()
    return mongoCustomerProfileDF

def writeDaataframetoPG(df, pgDB):
    df.write.format('jdbc').mode('append').option('url', pgConnUrl).option('user', pgUser)\
        .option('password', pgPassword).option('dbtable', pgDB).option("driver", "org.postgresql.Driver").save()

def extractLenderInfoDataframe(pgCustomerDF, mongoCustomerProfileDF):
    lenderCustomerDF = mongoCustomerProfileDF.join(pgCustomerDF, pgCustomerDF.mongoid == mongoCustomerProfileDF.user, how='left')
    lenderCustomerDF = lenderCustomerDF.select('lenderid',
                                               SparkFunction.col('id').alias('customer'),
                                               SparkFunction.col('createdAtLender').alias('createdat'),
                                               SparkFunction.col('updatedAtLender').alias('updatedAt'),
                                               SparkFunction.explode('lpinfo'))
    lenderCustomerDF = lenderCustomerDF.filter(~SparkFunction.col('customer').isNull())
    lenderCustomerDF = lenderCustomerDF.withColumn('lpinfo', lenderCustomerDF.col.rupeekid)
    lenderCustomerDF = lenderCustomerDF.select('customer', 'createdat', 'updatedat', 'lpinfo')
    lenderCustomerDF = attachIdToDataframe(lenderCustomerDF)
    lenderCustomerDF.printSchema()
    print('------ count -------')
    lenderCustomerDF.count()
    print('------ count -------')
    lenderCustomerDF.show(20, False)
    return lenderCustomerDF


def getUpdatedMonogoPhoneDataFrame(mongoPhonesDF, pgCustomerDF, recentUpdatedAt):
    updatedMongoPhones = mongoPhonesDF.filter(SparkFunction.col('updatedAt') > recentUpdatedAt)
    updatedMongoPhones = updatedMongoPhones.select('customer', SparkFunction.col('createdAt').alias('createdAtNew'),
                                                   SparkFunction.col('updatedAt').alias('updatedAtNew'), 'phone', 'isPrimary')
    joinedUpdatedPhonesDF = updatedMongoPhones.join(pgCustomerDF, updatedMongoPhones.customer == pgCustomerDF.mongoid, how='left')
    joinedUpdatedPhonesDF.show()



def extractCustomerSparkFunctionromMongoUser(mongoUserDF):
    customerDF = mongoUserDF.select('id', 'mongoid', 'createdAt', 'updatedAt')
    return customerDF


def attachIdToDataframe(dataframe):
    dataframe = dataframe.withColumn('id', uuidUDF())
    return dataframe


def writeMongoUserToPGCustomer(mode='Overwrite'):
    pass


def main():
    global spark
    spark = SparkSession.builder.appName("Mongo to Postgres TranSparkFunctionormation").getOrCreate()
    mongoUserDF = getMongoUserDataframe()
    pgCustomerDF = getPGDataframe(pgDbTable="customer_test")
    pgPhonesDF = getPGDataframe(pgDbTable="customerphones_test")
    # mongoUserWithPhoneDF.show(mongoUserWithPhoneDF.count(), False)
    customersDataFrame = extractPhoneFromMongoUsers(mongoUserDF)
    # print('------------ Mongo Users Dataframe -------------')
    # customersDataFrame.show(customersDataFrame.count(), False)
    # print('------------ PostGres Customers Dataframe ------------')
    # pgCustomerDF.show(pgCustomerDF.count(), False)
    recentUpdatedAt = getMostRecentUpdatedAt(pgCustomerDF)
    customersToBeUpsertedDF = getUpdatedMongoUserDataFrame(mongoUserDF=mongoUserDF, pgCustomersDF=pgCustomerDF,
                                                     recentUpdatedAt=recentUpdatedAt)
    # customersToBeUpsertedDF.show(customersToBeUpsertedDF.count(), False)
    mongoPhoneDF = extractPhoneFromMongoUsers(mongoUserDF)
    # mongoPhoneDF.show(20, False)
    # mongoUserDF.show(20, False)
    upsertPhones(mongoUserDF, pgCustomerDF, pgPhonesDF, recentUpdatedAt)
    # mongoUserDF.printSchema()
    # print(mongoUserDF.count())
    # print(pgCustomerDF.count())


if __name__ == '__main__':
    main()
