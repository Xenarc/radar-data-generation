import logging
import pandas as pd
import traceback
import json
import numpy as np
from pdw_to_pulse import pdw_to_pulse
from kafka import KafkaConsumer
import time
import matplotlib.pyplot as plt
import heapq

# from matplotlib import use as matplotlib_use
# matplotlib_use('Agg')

class Receiver:
  def __init__(self, time_dialation, kafka_servers: str, kafka_topic: str):
    self.time_dialation = time_dialation
    self.logger = logging.getLogger(__name__)
    self.kafka_servers = kafka_servers
    self.kafka_topic = kafka_topic
    self.is_running = True
    self.consumer = KafkaConsumer(
      bootstrap_servers=self.kafka_servers,
    )
    self.consumer.subscribe(topics=[self.kafka_topic])
    
    self.sample_rate = int(10e6/time_dialation) # 100Ms/s
    self.late_pdw_allowance = 2.0*time_dialation # A 2 second difference from oldest to newest PDW (min).
    self.accumulator_size = int(0.15*self.sample_rate*time_dialation) # 50 milliseconds' worth of samples
    max_pri_isolation_us = 100_000*time_dialation
    self.min_buffer_size = self.accumulator_size - max_pri_isolation_us*1e-6*self.sample_rate # samples
    
    self.fig, self.ax = plt.subplots()
  
  def receive(self):
    self.logger.info("Listening to pdws...")
    try:
      open('data.csv', 'w').close() # clear file
      with open('data.csv', 'ab') as datafile:
        accumulator = np.zeros((self.accumulator_size)) # start with a buffer for 100ms @ sample_rate
        acc_start_us = time.time_ns() / 1000
        pdws = []
        
        for message in self.consumer:
          self.logger.debug(message)
          if not self.is_running: break
          
          incoming_pdw = json.loads(message.value.decode('utf8'))
          heapq.heappush(pdws, (incoming_pdw['tot'], len(pdws), incoming_pdw))
          
          while(self.is_running and pdws[-1][2]['tot'] > pdws[0][2]['tot'] + self.late_pdw_allowance*1e6):
            pdw = heapq.heappop(pdws)[2]
            toa_offset_us = (pdw['tot'] - acc_start_us)
            
            if(toa_offset_us < 0):
              self.logger.warn(f"{pdw['name']} Skipping old pdw's. delay was {-toa_offset_us}us tot={pdw['tot']}")
              continue # Skip old pdw's which were sent before receiver began.
            
            # Figure out where to start merging the pulse
            toa_offset_samples = int((toa_offset_us*1e-6)*self.sample_rate)
            self.logger.debug(f"{pdw['name']} toa_offset_us={toa_offset_us}us tot={pdw['tot']} ({toa_offset_samples}/{accumulator.size}) buffer alloc={100*toa_offset_samples/accumulator.size}%")
            
            # push pulse to accumulator
            pulse = pdw_to_pulse(pdw, self.sample_rate)
            tof_offset_samples = toa_offset_samples + len(pulse)
            accumulator[toa_offset_samples:tof_offset_samples] += pulse
             
            if(toa_offset_samples > self.min_buffer_size):
              num_samples_to_emit = toa_offset_samples
              time_delta_of_emission = (num_samples_to_emit/self.sample_rate)*1e6
              
              samples_to_write = accumulator[:num_samples_to_emit-1]
              self.write_samples(samples_to_write, acc_start_us, datafile)
              
              accumulator = np.pad(accumulator[num_samples_to_emit:], (0,num_samples_to_emit), 'constant')
              
              now_us = time.time_ns()/1000
              self.logger.info(f'{(now_us - acc_start_us)*1e-6}s late')
              acc_start_us += time_delta_of_emission

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
    plt.pause(0.0001)  # Pause for a short time to allow the plot to update

  def stop(self):
    self.logger.info("Receiver stopping...")
    self.is_running = False
