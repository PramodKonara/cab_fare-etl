import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

class AgeRangeTransform(beam.DoFn):
    def process(self, element):
        age = int(element['age'])
        if 18 <= age <= 25:
            element['age_range'] = '18-25'
        elif 26 <= age <= 35:
            element['age_range'] = '26-35'
        elif 36 <= age <= 45:
            element['age_range'] = '36-45'
        elif 46 <= age <= 55:
            element['age_range'] = '46-55'
        else:
            element['age_range'] = '56+'
        return [element]

class AggregateFareTransform(beam.DoFn):
    def process(self, element):
        key = (element['age_range'], element['state'])
        fare = float(element['fare'])
        return [(key, fare)]