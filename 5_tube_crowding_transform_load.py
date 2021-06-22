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
    #The TFL API gives data for an average day
    #To make visualization easier, pretend the data is for an actual day
    synthetic_dates = {
      "MON":"2021-06-07", 
      "TUE":"2021-06-08",
      "WED":"2021-06-09",
      "THU":"2021-06-10",
      "FRI":"2021-06-11",
      "SAT":"2021-06-12",
      "SUN":"2021-06-13",
      }
    o = json.loads(element)
    measure_date = synthetic_dates[o["dayOfWeek"]]
    measurements = []
    for t in o["timeBands"]:
      measure = {}
      measure["tube_station_id"] = o["tube_station_id"]
      measure["measurement_ts"] = "{} {}:00.00 UTC".format(measure_date, t["timeBand"].split("-")[0])
      measure["crowding_percentage_of_base_line"] = t["percentageOfBaseLine"]
      measurements.append(measure)
    return measurements

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
        "tfl-stats:tfl_stats.tube_crowding",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()