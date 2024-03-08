import logging
import pandas as pd
import traceback
import json
import numpy as np
from pdw_to_pulse import pdw_to_pulse
from kafka import KafkaConsumer
import time
import matplotlib.pyplot as plt
# from matplotlib import use as matplotlib_use
# matplotlib_use('Agg')

class Receiver:
  def __init__(self, kafka_servers: str, kafka_topic: str):
    self.logger = logging.getLogger(__name__)
    self.kafka_servers = kafka_servers
    self.kafka_topic = kafka_topic
    self.is_running = True
    self.consumer = KafkaConsumer(
      bootstrap_servers=self.kafka_servers,
    )
    self.consumer.subscribe(topics=[self.kafka_topic])
    
    self.sample_rate = int(300e6) # 100Ms/s
    self.start_dead_time_us = 50_000 # start with a 100ms head start
    self.max_lag_samples = int(15e-6*self.sample_rate) # pdw's can be 15us late max
    self.accumulator_size = int(0.03*self.sample_rate) # 50 milliseconds' worth of samples
    max_pri_isolation_us = 10_000
    self.min_buffer_size = self.accumulator_size - max_pri_isolation_us*1e-6*self.sample_rate # samples
    
    self.fig, self.ax = plt.subplots()
  
  def receive(self):
    self.logger.info("Listening to pdws...")
    try:
      open('data.csv', 'w').close() # clear file
      with open('data.csv', 'a') as datafile:
        accumulator = np.zeros((self.accumulator_size)) # start with a buffer for 100ms @ sample_rate
        acc_start_us = time.time_ns() / 1000 + self.start_dead_time_us
        for message in self.consumer:
          self.logger.debug(message)
          if not self.is_running: break
          
          pdw = json.loads(message.value.decode('utf8'))
          
          toa_offset_us = (pdw['tot'] - acc_start_us)
          if(toa_offset_us < 0):
            self.logger.info(f"Skipping old pdw's. delay was {-toa_offset_us}us which is greater than max allowed {(self.max_lag_samples/self.sample_rate)*1e6}us")
            continue # Skip old pdw's which were sent before receiver began.
          
          toa_offset_samples = int((toa_offset_us*1e-6)*self.sample_rate)
          self.logger.info(f"toa_offset_us={toa_offset_us}us ({toa_offset_samples}/{accumulator.size}) buffer alloc={100*toa_offset_samples/accumulator.size}%")
          
          # if(toa_offset_samples > accumulator.size):
          #   self.logger.info(f"Skipping pulse! it's too far ahead")
          
          pulse = pdw_to_pulse(pdw, self.sample_rate)
          accumulator[toa_offset_samples:toa_offset_samples + len(pulse)] += pulse
          
          max_safe_samples = toa_offset_samples - self.max_lag_samples
          
          if(max_safe_samples > self.min_buffer_size):
            num_samples_to_emit = max_safe_samples
            time_delta_of_emission = (num_samples_to_emit/self.sample_rate)*1e6
            
            self.logger.info(f'Emitting {num_samples_to_emit} samples! ({time_delta_of_emission}us)')
            
            samples_to_write = accumulator[:num_samples_to_emit-1]
            self.write_samples(samples_to_write, acc_start_us, datafile)
            
            accumulator = np.pad(accumulator[num_samples_to_emit:], (0,num_samples_to_emit), 'constant')
            acc_start_us += time_delta_of_emission
            
            now_us = time.time_ns()/1000
            self.logger.info(f'{(now_us - acc_start_us)*1e-6}s late')

    except Exception:
      print(f"An error occurred in {__class__.__name__}")
      self.logger.error(traceback.format_exc())
    
    self.consumer.close()
    self.logger.info("Receiver stopped.")

  def write_samples(self, samples, timestamp, datafile):
    np.savetxt(datafile, samples, delimiter=',')
    # np.save(datafile, samples)
    return
    self.ax.clear()
    self.ax.plot(range(len(samples)), samples)
    self.ax.set_xlabel('Time')
    self.ax.set_ylabel('Values')
    self.ax.set_title('Real-time Time Series Plot')
    self.ax.set_ylim([-2, 2])
    self.fig.canvas.draw()
    plt.pause(0.01)  # Pause for a short time to allow the plot to update

  def stop(self):
    self.logger.info("Receiver stopping...")
    self.is_running = False
