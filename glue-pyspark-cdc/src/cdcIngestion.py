import pyspark.sql.functions as F


# change so that each method recives all its input as parameters.
class cdcIngestion:
    def __init__(
        self,
        sparkSession=None,
        high_water_column=None,
        source_high_watermark=None,
        target_high_watermark=None,
        unique_key=None,
        source_data=None,
        target_data=None,
        update_data=None
    ):
        self.spark = sparkSession
        self.high_water_column = high_water_column
        self.source_high_watermark = source_high_watermark
        self.target_high_watermark = target_high_watermark
        self.unique_key = unique_key
        self.target_data = target_data
        self.source_data = source_data
        self.update_data = update_data

    def get_source_high_watermark(
        self,
        spark=None,
        high_water_column=None,
        source_data=None,
    ):
        if spark is None:
            spark = self.spark
        if high_water_column is None:
            high_water_column = self.high_water_column
        if source_data is None:
            source_data = self.source_data

        if source_data is not None:
            self.source_high_watermark = (
                source_data.select(high_water_column)
                .agg(F.max(high_water_column))
                .collect()[0][0]
            )
        return self.source_high_watermark


    def get_target_high_watermark(
            self,
            spark=None,
            high_water_column=None,
            target_data=None,
            ):
        if spark is None:
            spark = self.spark
        if target_data is None:
            target_data = self.target_data
        if high_water_column is None:
            high_water_column = self.high_water_column

        if target_data is not None:
            self.target_high_watermark = (
                    target_data.select(high_water_column)
                    .agg(F.max(high_water_column))
                    .collect()[0][0]
                    )
        return self.target_high_watermark

    def get_high_watermark(
        self,
        spark=None,
        high_water_column=None,
        source_data=None,
        target_data=None,
    ):
        if spark is None:
            spark = self.spark
        if high_water_column is None:
            high_water_column = self.high_water_column
        if source_data is None:
            source_data = self.source_data
        if target_data is None:
            target_data = self.target_data

        self.get_source_high_watermark(spark, high_water_column, source_data)
        self.get_target_high_watermark(spark, high_water_column, target_data)

        return self.source_high_watermark, self.target_high_watermark

    def get_updates(
            self,
            spark=None,
            high_water_column=None,
            target_high_watermark=None,
            source_data=None,
            ):
        if spark is None:
            spark = self.spark
        if high_water_column is None:
            high_water_column = self.high_water_column
        if target_high_watermark is None:
           target_high_watermark = self.target_high_watermark
        if source_data is None:
            source_data = self.source_data

        self.update_data = source_data.filter(F.col(high_water_column)>target_high_watermark)

        return self.update_data

    def process_updates(
            self,
            spark=None,
            update_data=None,
            unique_key=None,
            ):
        if spark is None:
            spark=self.spark
        if update_data is None:
            update_data = self.update_data
        if unique_key is None:
            unique_key = self.unique_key

        self.deletes = update_data.filter(F.col('op')=='D').dropDuplicates(unique_key)

        self.upsert_data = None

        return self.deletes, self.upsert_data
