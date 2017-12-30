from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
with beam.Pipeline(options=PipelineOptions()) as p:
  #lines = p | 'Create' >> beam.Create(['cat\tdog', 'snake\tcat', 'dog', 'cat\tdog'])
  lines = p | 'Create' >> beam.Create(['A\t10\nA\t10\nB\t25'])
  counts = (
    lines
    | 'Records' >> beam.FlatMap(lambda x: x.split('\n'))
    | 'Key Values' >> beam.Map(lambda x: x.split('\t'))
    #| 'Cast the Values' >> beam.Map(lambda x: int(x[1]))
    #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
    | 'Value' >> beam.Map(lambda x: (x[0], int(x[1])))
    #| 'GroupAndSum' >> beam.GroupByKey()
    | 'Sum' >> beam.CombinePerKey(sum)
  )
  #counts | 'Print' >> beam.ParDo(lambda (w, c): print('%s: %s' % (w, c)))
  #counts | 'Print' >> beam.ParDo(lambda (w, c): print('{0}: {1}'.format(w, c)))
  counts | 'Print' >> beam.ParDo(lambda x: print(x))
  counts | beam.io.WriteToText('gs://scrap-data-location-test/test_output/file.txt')
  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  counts | 'Write' >> beam.io.WriteToBigQuery(
    known_args.output,
    schema='month:STRING, tornado_count:INTEGER',
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
#p.run()
