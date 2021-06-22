import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class StationExtractingDoFn(beam.DoFn):
	def process(self, element):
		o = json.loads(element)
		return [{
             "tube_line_id":o["lineId"],
             "tube_station_id":o["naptanId"],
             "tube_station_name":o["commonName"],
             "lat":o["lat"],
             "lon":o["lon"],
             }]

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      required=True,
      help='Input file to process.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


  with beam.Pipeline(options=pipeline_options) as p:
    (p | 'Read' >> ReadFromText(known_args.input)
       | 'Extract' >> beam.ParDo(StationExtractingDoFn())
       | 'Write'  >> WriteToBigQuery(
        "tfl-stats:tfl_stats.tube_stations",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()