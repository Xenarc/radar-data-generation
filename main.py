import logging
from scenario import Scenario

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  logging.getLogger('kafka').setLevel(logging.ERROR)
  scenario = Scenario.load_scenario_from_yaml('scenario.yaml')
  scenario.run()
