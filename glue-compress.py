try:
    import sys
    from awsglue.transforms import *
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Some Moduels are Missing : {} ".format(e))


class InitializeGlue(object):
    def __init__(self):
        self.args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.job.init(self.args["JOB_NAME"], self.args)


class GLueTransformation(InitializeGlue):
    def __init__(
            self,
            groupSize="1048576",
            format="json",
            input_location="",
            compression="snappy",
            output_format="parquet",
            output_location="",
    ):

        InitializeGlue.__init__(self)

        self.groupSize = groupSize
        self.format = format
        self.input_location = input_location
        self.output_format = output_format
        self.compression = compression
        self.output_format = output_format
        self.output_location = output_location

    def run(self):

        df = self.__step_1_read_data_set()
        self.__step_2_write_data_set(data_source_frame=df)

        self.job.commit()

    def __step_1_read_data_set(self):
        data_source_frame = self.glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options={
                "paths": [self.input_location],
                "recurse": True,
                "groupFiles": "inPartition",
                "groupSize": self.groupSize,
            },
            format=self.format,
            transformation_ctx="data_source_frame",
        )
        return data_source_frame

    def __step_2_write_data_set(
            self, data_source_frame,
    ):

        datasink = self.glueContext.write_dynamic_frame.from_options(
            frame=data_source_frame,
            connection_type="s3",
            connection_options={"path": self.output_location},
            format=self.output_format,
            transformation_ctx="datasink",
            format_options={"compression": self.compression},
        )


if __name__ == "__main__":

    helper = GLueTransformation(
        input_location="XXXXXXXXXXXX",
        output_location="XXXXXXXXXXXX",
    )
    helper.run()
