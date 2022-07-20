import apache_beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputgcs",
                        dest="inputgcs",
                        required=True,
                        help="Please provide Input GCS path")

    parser.add_argument("--outputpath",
                        dest="outputpath",
                        required=True,
                        help="Please provide output GCS path")



    Known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)

    pipeline_options.view_as(SetupOptions).Save_main_session=True



