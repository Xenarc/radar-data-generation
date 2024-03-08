from abc import ABC, abstractmethod

class IRadar(ABC):
  @abstractmethod
  def run(self): pass
  
  @abstractmethod
  def stop(self): pass
