import yaml
import traceback
import time
import importlib.util
from concurrent.futures import ThreadPoolExecutor
import logging
import signal
import sys
from radar import IRadar

from receiver import Receiver

class Scenario:
  def __init__(self, radars: [IRadar], time_dialation: float):
    self.radars: Dict[str, IRadar] = radars
    self.logger = logging.getLogger(__name__)
    self.receiver = Receiver(time_dialation, "localhost:9092", "pdw")
    self.is_running = True
    signal.signal(signal.SIGINT, self.stop)
  
  @staticmethod
  def load_scenario_from_yaml(file_path: str):
    logger = logging.getLogger(__class__.__name__)
    logger.info(f"Creating Scenario from {file_path}")
    with open(file_path, 'r') as file:
      loaded_yaml = yaml.safe_load(file)
      scenario_data = loaded_yaml['radars']
      time_dialation = loaded_yaml['time_dialation']
    
    radars = {}
    for radar_name, radar_info in scenario_data.items():
      module_name = radar_info['module']
      class_name = radar_info['class']
      parameters = radar_info.get('parameters', {})
      
      module = importlib.import_module(module_name)
      radar_class = getattr(module, class_name)
      radar_instance = radar_class(time_dialation, **parameters)
      radars[radar_name] = radar_instance
    logger.info(f"Created Scenario with {len(radars.values())} radars")
    return Scenario(radars, time_dialation)
  
  def run(self):
    self.logger.info("Running Scenario")
    with ThreadPoolExecutor() as executor:
      try:
        # Start radars
        for radar in self.radars.values():
          executor.submit(radar.run)
        self.logger.info("All radars invoked")
        
        # Start receiver
        self.receiver.receive()
        # executor.submit(self.receiver.receive)
        
        self.logger.info("Scenario fully running...")
        while self.is_running:
          time.sleep(0.02)
      
      except Exception as e:
        print(f"An error occurred in {__class__.__name__}")
        self.logger.error(traceback.format_exc())
      
      self.logger.info(f"Stopping...")
      # Stop the radars
      for radar_name, radar in self.radars.items():
        self.logger.info(f"Stopping radar {radar_name}...")
        radar.stop()
      
      # Stop the receiver
      self.logger.info(f"Stopping receiver...")
      self.receiver.stop()
      
    self.logger.info("Scenario Stopped. Exiting...")
  
  def stop(self, sig, frame):
    self.logger.info("Received KeyboardInterrupt. Exiting gracefully.")
    self.is_running = False
