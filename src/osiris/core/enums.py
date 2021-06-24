"""
Contains enums for pipelines
"""
from enum import Enum

TimeResolution = Enum('TimeResolution', 'NONE YEAR MONTH DAY HOUR MINUTE')

Horizon = Enum('Horizon', 'YEARLY MONTHLY DAILY HOURLY MINUTELY')
