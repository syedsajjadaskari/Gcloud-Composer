import argparse
import logging
import ntpath
import re

import apache_beam as beam
from apache_beam.options import pipeline_options

class RowTransformer(object):
    def __init__(self,delimeter,header,filename,load_dt):
        self.delimeter=delimeter
        self.keys=re.split(',',header)
        self.filename=filename
        self.load_dt=load_dt
    def parse(self,row):
        Values = re.split(self.delimeter,re.sub(r'[\r\n"]','',row))
        row=dict(list(zip(self.keys,Values)))
        row['filename']=self.filename
        row['load_dt']=self.load_dt
        return row
'''this main function which creates pipeline an runs it.'''
def run(argv=None):
    parser=argparse.ArgumentParser()
    #add the arguments for the specific dataflow job
    parser.add_argument('--input',dest='input',required=True,
                        help='Input file to read. This can be loacal file or ''a filr in a google storage bucket')
    parser.add_argument('--output',dest='output',required=True,
                        help='Output BQ table to write results to .')
    
    parser.add_argument('--delimiter', dest='delimiter', required=False,
                        help='Delimiter to split input records.',
                        default=',')

    parser.add_argument('--fields', dest='fields', required=True,
                        help='Comma separated list of field names.')

    parser.add_argument('--load_dt', dest='load_dt', required=True,
                        help='Load date in YYYY-MM-DD format.')
    know_args,pipeline_args=parser.parse_known_args(argv)
    row_transformation=RowTransformer(delimeter=know_args.delimiter,
                                      header=know_args.fields,
                                      filename=ntpath.basename(know_args.input),
                                      load_dt=know_args.load_dt)
    p_opts=pipeline_options.PipelineOptions(pipeline_args)
    #inittialize the pipeline using the pipeline arguments passed 
    #storre temp file
    with beam.Pipeline(options=p_opts) as pipline:
        #read the file and processiong start from hear
        rows = pipline | "Read from text file" >> beam.io.ReadFromText(know_args.input)

        #this stage of the pipeline translates from a delimeiter single row
        dict_rows=rows | "convert to Bigquery row" >> beam.Map(lambda r: row_transformation.parse(r))

        dict_records | "write ato bigQuey" >> beam.io.Write(
            beam.io.BigQuerySink(know_args.output,
                                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )
if __name__=='__main__':
    logging.getLogger.setLevel(logging.INFO)
    run()