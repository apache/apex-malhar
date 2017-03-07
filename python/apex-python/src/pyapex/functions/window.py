#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

class TriggerType:
  EARLY = 1
  ON_TIME = 2
  LATE = 3
  @staticmethod
  def isValid(trigger_type):
    if trigger_type < TriggerType.EARLY:
      raise Exception("Incorrect Trigger Type")
    if trigger_type > TriggerType.LATE:
      raise Exception("Incorrect Trigger Type")
    return True

class AccumulationMode:
  DISCARDING = 1
  ACCUMULATING = 2
  ACCUMULATING_AND_RETRACTING = 3
  @staticmethod
  def isValid(type):
    if type < TriggerType.DISCARDING:
      raise Exception("Incorrect Accumulation Mode")
    if type > TriggerType.ACCUMULATING_AND_RETRACTING:
      raise Exception("Incorrect Accumulation Mode")
    return True

class TriggerOption(object):
  triggers =  []
  accumulation_mode = None
  firingOnlyUpdatedPanes = False
  @staticmethod
  def at_watermark():
    triggerOption = TriggerOption()
    trigger = Trigger(TriggerType.ON_TIME)
    triggerOption.triggers.append(trigger)
    return triggerOption

  def withEarlyFiringsAtEvery(self,*args,**kwargs):
    trigger = None
    if 'count' in kwargs:
      trigger = CountTrigger(TriggerType.EARLY, kwargs['count'])
    if 'duration' in kwargs:
      trigger = TimeTrigger(TriggerType.EARLY, kwargs['duration'])
    if trigger is None:
      raise Exception("Unsufficent for trigger")
    self.triggers.append(trigger)
    return self

  def withLateFiringsAtEvery( self, *args, **kwargs ):
    trigger = None
    if 'count' in kwargs:
      trigger = CountTrigger(TriggerType.LATE, kwargs['count'])
    if 'duration' in kwargs:
      trigger = TimeTrigger(TriggerType.LATE, kwargs['duration'])
    if trigger is None:
      raise Exception("Unsufficent for trigger")
    self.triggers.append(trigger)
    return self

  def discardingFiredPanes(self):
    self.accumulation_mode = AccumulationMode.DISCARDING
    return self

  def accumulatingFiredPanes(self):
    self.accumulation_mode = AccumulationMode.ACCUMULATING
    return self

  def accumulatingAndRetractingFiredPanes(self):
    self.accumulation_mode = AccumulationMode.ACCUMULATING_AND_RETRACTING
    return self

  def firingOnlyUpdatedPanes(self):
    self.firingOnlyUpdatedPanes = True
    return self

  @staticmethod
  def get_java_trigger_options(trigger_option, gateway):
    _jtrigger_option = None
    for trigger in trigger_option.triggers:
        if trigger.trigger_type == TriggerType.ON_TIME:
          _jtrigger_option = gateway.jvm.TriggerOption.AtWatermark()
        elif trigger.trigger_type == TriggerType.EARLY:
          if isinstance(trigger, TimeTrigger):
            _jduration = gateway.jvm.Duration(trigger.duration)
            _jtrigger_option = _jtrigger_option.withEarlyFiringsAtEvery(_jduration)
          else:
            _jcount = gateway.jvm.Duration(trigger.count)
            _jtrigger_option = _jtrigger_option.withEarlyFiringsAtEvery(_jcount)
        elif trigger.trigger_type == TriggerType.LATE:
          if isinstance(trigger, TimeTrigger):
            _jduration = gateway.jvm.Duration(trigger.duration)
            _jtrigger_option = _jtrigger_option.withLateFiringsAtEvery(_jduration)
          else:
            _jcount = gateway.jvm.Duration(trigger.count)
            _jtrigger_option = _jtrigger_option.withLateFiringsAtEvery(_jcount)
    return _jtrigger_option

class Trigger(object):
  trigger_type = None

  def __init__(self,trigger_type):
    self.trigger_type = trigger_type

class TimeTrigger(Trigger):
  duration = None

  def __init__(self, trigger_type, duration):
    super(TimeTrigger,self).__init__(trigger_type)
    self.duration = duration

class CountTrigger(Trigger):
  count = None

  def __init__(self, trigger_type, count):
    super(CountTrigger, self).__init__(trigger_type)
    self.count = count





