import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from transforms import AgeRangeTransform, AggregateFareTransform

def run_pipeline():

    headers = ['date', 'name', 'age', 'city', 'state', 'fare', 'gender', 'occupation']

    table_schema = {
        'fields': [
            {'name': 'age_range', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'total_fare', 'type': 'FLOAT'}
        ]
    }

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'nth-hybrid-405305'  # Replace with your actual project ID
    google_cloud_options.job_name = 'Trip-Details-Pipeline'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://uk-property-data/ETL-1/stage'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://uk-property-data/ETL-1/temp'  # Replace with your GCS Temporary files bucket


    with beam.Pipeline(options=options) as pipeline:
        # Read from the input CSV file
        lines = pipeline | 'ReadFromCSV' >> ReadFromText('gs://cab-source-data/input.csv', skip_header_lines=1)

        # Parse CSV lines into dictionaries
        parsed_data = (lines
                       | 'ParseCSV' >> beam.Map(lambda line: dict(zip(headers, line.split(',')))))

        # Apply age range transformation
        age_ranged_data = (parsed_data
                           | 'AgeRangeTransform' >> beam.ParDo(AgeRangeTransform()))

        # Aggregate fare by age range and state
        aggregated_data = (age_ranged_data
                           | 'AggregateFareTransform' >> beam.ParDo(AggregateFareTransform())
                           | 'SumFare' >> beam.CombinePerKey(sum))

        # Write the results to BigQuery
        _ = (aggregated_data
             | 'FormatOutput' >> beam.Map(lambda element: {
                    'age_range': element[0][0],
                    'state': element[0][1],
                    'total_fare': round(element[1], 2)
                })
             | 'WriteToBigQuery' >> WriteToBigQuery(
                    table='nth-hybrid-405305:TripData.TripFare',
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
             )

run_pipeline()