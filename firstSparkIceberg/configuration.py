LOCAL_CONFIG = {

    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue.warehouse": "s3://spark-iceberg-local-br/spark-iceberg-db-local/",
    "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"

}
