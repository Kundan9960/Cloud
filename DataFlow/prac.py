
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.column import *

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("gfgf").getOrCreate()
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    rdd1=spark.sparkContext.parallelize(zip([1,1,1,1,0,0,0,0]))

    print(rdd1.collect())


    df=rdd1.toDF()

    df.printSchema()
    df.show()

    Windospec=Window.partitionBy("_1").orderBy("_1")

    new=df.withColumn("Row_number",row_number().over(Windospec)).orderBy(desc("Row_number")).select(["_1"]).show()




    # new=df.groupby(col("Row_number"))






