import logging
import traceback
import json
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka

class Receiver:
  def __init__(self, kafka_servers: str, kafka_topic: str):
    self.logger = logging.getLogger(__name__)
    self.kafka_servers = kafka_servers
    self.kafka_topic = kafka_topic
    self.pipeline_result = None
    
    self.pipeline = beam.Pipeline(options=PipelineOptions(
      streaming=True,
      runner="DirectRunner",
      experiments=["use_deprecated_read"]
    ))
    self.pipeline.__enter__()
    _ = (self.pipeline
        | 'Read from Kafka' >> ReadFromKafka(
            consumer_config={
              'bootstrap.servers': self.kafka_servers,
              'auto.offset.reset': 'earliest',
            },
            topics=[self.kafka_topic]
        )
        | 'Log' >> beam.Map(logging.info)
        
        # | 'Decode message' >> beam.Map(lambda message: json.loads(message.value.decode('utf-8')))
        # | 'Process message' >> beam.ParDo(ProcessMessage())
    )

  def receive(self):
    try:
      self.logger.info("Running...")
      self.pipeline_result = self.pipeline.run() 
      self.logger.info("Listening to pdws...")
      self.pipeline_result.wait_until_finish()
      self.logger.info("Finished.")
    except Exception as e:
      self.logger.error(traceback.format_exc())
      
  def stop(self):
    self.logger.info("Receiver stopping...")
    if self.pipeline_result is not None:
      try:
        self.pipeline_result.cancel()
      except NotImplementedError: pass
    else:
      self.logger.info("Receiver already stopped.")
    
    self.logger.info("Stopping pipeline")
    self.pipeline.__exit__()
    self.logger.info("Receiver stopped.")
    
