from random import randint
import logging
import traceback
from radar import IRadar
from kafka import KafkaProducer
import numpy as np
import json
import time

class SingleModeRadar(IRadar):
  def __init__(self,
                name,
                pri_us,
                pulse_duration_us,
                rf_mhz,
                amplitude,
                kafka_server,
                topic):
    self.name = name
    self.pri_us = float(pri_us)
    self.pulse_duration_us = float(pulse_duration_us)
    self.rf_mhz = float(rf_mhz)
    self.amplitude = float(amplitude)
    self.running = True
    self.kafka_server = kafka_server
    self.topic= topic
    self.producer = KafkaProducer(bootstrap_servers=self.kafka_server)
    self.logger = logging.getLogger(f"{__name__}:{self.name}")
  
  def run(self):
    try:
      self.logger.info("Starting radar")
      self.running = True
      previous_pri_time_us = time.time_ns()/1000 + randint(0, self.pri_us)  # calculate next execution time
      while self.running:
        while self.running and previous_pri_time_us <= previous_pri_time_us + self.pri_us:
          self.publish_pdw({
            "tot": previous_pri_time_us,
            "name": self.name,
            "rf": self.rf_mhz + np.random.normal(0, 0.05),  # 50kHz RF drift
            "pri": self.pri_us,
            "pd": self.pulse_duration_us,
            "amplitude": self.amplitude
          })
          previous_pri_time_us += self.pri_us  # increment next execution time by PRI in seconds
        time.sleep((previous_pri_time_us/5)*1e-6)  # sleep until next execution time
    except Exception as e:
      print(f"An error occurred in {__class__.__name__}")
      self.logger.error(traceback.format_exc())
    
    self.producer.close()
    self.logger.info("Radar stopped.")
  
  def publish_pdw(self, pdw):
    self.producer.send(self.topic, json.dumps(pdw).encode())
    self.logger.debug(f"Published PDW to Kafka: {self.topic}")
    self.producer.flush()
  
  def stop(self):
    self.running = False
