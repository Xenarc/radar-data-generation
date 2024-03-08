import numpy as np

def pdw_to_pulse(pdw, sample_rate):
  rf = pdw['rf'] * 1e6
  pd = pdw['pd'] * 1e-6
  amplitude = pdw['amplitude']
  
  num_samples = int(pd * sample_rate)
  t = np.arange(num_samples)
  pulse = amplitude * np.sin(2 * np.pi * rf * t / sample_rate)
  
  return pulse
