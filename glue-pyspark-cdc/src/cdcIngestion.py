import pyspark.sql.functions as F


# change so that each method recives all its input as parameters.
class cdcIngestion:
    def __init__(
        self,
        sparkSession,
        high_water_column="",
        source_data=None,
        target_data=None,
    ):
        self.spark = sparkSession
        self.high_water_column = high_water_column
        self.source_high_watermark = None
        self.target_high_watermark = None
        self.target_data = target_data
        self.source_data = source_data

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
            self.source_high_water_mark = (
                source_data.select(high_water_column)
                .agg(F.max(high_water_column))
                .collect()[0][0]
            )
        return self.source_high_water_mark


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

    def get_high_water_mark(
        self,
        spark=None,
        source_data=None,
        high_water_mark=None,
        high_water_column=None,
        target_data=None,
    ):
        if spark is None:
            spark = self.spark
        if source_data is None:
            source_data = self.source_data
        if high_water_column is None:
            high_water_column = self.high_water_column

        if source_data is not None:
            self.source_high_water_mark = (
                source_data.select(high_water_column)
                .agg(F.max(high_water_column))
                .collect()[0][0]
            )
        else:
            pass

        return self.source_high_water_mark, self.target_high_water_mark

    def run(self):
        self.get_high_water_mark()
