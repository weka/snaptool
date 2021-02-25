

import time, datetime, dateutil
from logging import debug, info, warning, error, critical, getLogger, DEBUG, StreamHandler
import logging
from threading import Lock

log = getLogger(__name__)

class SnapSchedule():
    def __init__(self, name, monthly, weekly, daily, hourly):
        self.name = name
        self.schedule = [monthly, weekly, daily, hourly]
        self.nextsnap = None

    # NOTE - this returns an OBJECT
    def next_snap(self, now):
        if self.nextsnap == None or now > self.nextsnap.nextsnap_dt:     
            # self.nextsnap is old, update it
            log.debug(f"updating snaptime")
            earliest = None
            for sched in self.schedule:
                # if retain is 0, it means we're not taking any snaps
                if sched.retain > 0:
                    log.debug(f"sched = {sched}, earliest = {earliest}")
                    if earliest == None:
                        earliest = sched
                    elif sched.next_snaptime(now) < earliest.next_snaptime(now):
                        earliest = sched
            self.nextsnap = earliest
        return self.nextsnap    # NOTE - this returns an OBJECT

class BaseSchedule(object):
    def __init__(self, name):
        self.nextsnap_dt = datetime.datetime(2000,4,28,1,13)    # son's Bday. ;)
        self.name = name
        self.next_snaptime(datetime.datetime(2000,4,28,1,13)) # initialize algorithm
        #log.debug(f"name is {self.name}")

    def __eq__(self, other):
        if other == None: # not sure how None works here... is this circular?
            return False
        else:
            return self.nextsnap_dt == other.nextsnap_dt

    def __gt__(self, other):
        return self.nextsnap_dt > other.nextsnap_dt

    def __lt__(self, other):
        return self.nextsnap_dt < other.nextsnap_dt

    def __str__(self):
        return(f"{self.name} {self.nextsnap_dt.isoformat()}")

class MonthlySchedule(BaseSchedule):
    def __init__(self, date=1, time=0, retain=6, upload=False):
        self.date = date
        self.hour = int(time/100)
        self.minute = int(time - (self.hour *100))
        self.retain = retain
        self.upload = upload
        BaseSchedule.__init__(self, "monthly")

    def next_snaptime(self, now):
        log.debug(f"(monthly)now={now}, self.nextsnap_dt={self.nextsnap_dt}")
        if now > self.nextsnap_dt:
            # nextsnap_dt is in the past
            log.debug(f"date={self.date}, hour={self.hour},minute={self.minute}")
            temp_datetime = datetime.datetime(now.year, now.month, self.date, self.hour, self.minute)
            if now <= temp_datetime:
                # it's in the near future (this month, but hasn't happened yet)
                self.nextsnap_dt = temp_datetime
            else:
                # this month's passed already.  When is next month's?
                # bump into next month, but don't overshoot February
                time_delta = datetime.timedelta(days=32-now.day) 
                temp_date = now + time_delta    # do this to make sure it rolls to next year, if needed
                target_datetime = datetime.datetime(temp_date.year, temp_date.month, self.date, self.hour, self.minute)
                self.nextsnap_dt = target_datetime
        log.debug(f"(monthly), returning self.nextsnap_dt={self.nextsnap_dt}")
        return self.nextsnap_dt

class WeeklySchedule(BaseSchedule):
    def __init__(self, weekday=6, time=0, retain=8, upload=False):  # time is 1700 for 5pm
        self.weekday = weekday
        self.hour = int(time/100)
        self.minute = int(time - (self.hour *100))
        self.retain = retain
        self.upload = upload
        BaseSchedule.__init__(self, "weekly")

    def next_snaptime(self, now):
        if now > self.nextsnap_dt:     
            now_weekday = now.weekday()

            log.debug(f"now_weekday={now_weekday}, self.weekday= {self.weekday}")
            if now_weekday == self.weekday:
                temp_datetime = datetime.datetime(now.year, now.month, now.day, self.hour, self.minute)
                log.debug(f"scheduled today at {temp_datetime}")
                if now <= temp_datetime:
                    log.debug(f"hasn't happened yet")
                    # today, but hasn't happened yet
                    self.nextsnap_dt = temp_datetime
                    return self.nextsnap_dt
                    #continue
            log.debug(f"already happened")
            day_delta = 7 + self.weekday - now_weekday
            log.debug(f"already happened. day_delta = {day_delta}")
            if day_delta > 7:
                # it's later this week, not next week
                day_delta -= 7
            log.debug(f"day_delta = {day_delta}")
            temp_date = now + datetime.timedelta(days=day_delta)
            log.debug(f"temp_date = {temp_date}")
            target_datetime = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, self.hour, self.minute)
            log.debug(f"target_datetime = {target_datetime}")
            self.nextsnap_dt = target_datetime
        log.debug(f"(weekly), returning self.nextsnap_dt={self.nextsnap_dt}")
        return self.nextsnap_dt


class DailySchedule(BaseSchedule):
    def __init__(self, time=0, start_day=0, stop_day=6, retain=14, upload=False):   # time is 1700 for 5pm
        self.hour = int(time/100)
        self.minute = int(time - (self.hour *100))
        self.start_day = start_day
        self.stop_day = stop_day
        self.retain = retain
        self.upload = upload
        BaseSchedule.__init__(self, "daily")

    def next_snaptime(self, now):
        if now > self.nextsnap_dt:     
            temp_datetime = datetime.datetime(now.year, now.month, now.day, self.hour, self.minute)
            if now <= temp_datetime:
                # hasn't happened yet
                self.nextsnap_dt = temp_datetime
            else:
                # schedule for tomorrow
                temp_date = now + datetime.timedelta(days=1)
                self.nextsnap_dt = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, self.hour, self.minute)
        log.debug(f"(daily), returning self.nextsnap_dt={self.nextsnap_dt}")
        return self.nextsnap_dt

class HourlySchedule(BaseSchedule):
    # default is mon-fri, 9-5, top of hour
    def __init__(self, start_day=0, stop_day=4, start_time=900, stop_time=1700, snap_minute=0, name_format="standard", retain=10, upload=False):    # snap_minute == mins past the hour to snap
        self.start_day = start_day
        self.stop_day = stop_day
        self.start_time = start_time
        self.start_hour = int(start_time/100)
        self.start_minute = int(start_time - (self.start_hour *100))
        self.stop_time = stop_time
        self.stop_hour = int(stop_time/100)
        self.stop_minute = int(stop_time - (self.stop_hour *100))
        self.snap_minute = snap_minute
        self.retain = retain
        self.upload = upload
        BaseSchedule.__init__(self, 'hourly')

    def next_snaptime(self, now):
        log.debug(f"now is {now.isoformat()}")
        if now > self.nextsnap_dt:     
            log.debug(f"Recalculating next snaptime")
            now_weekday = now.weekday()
            log.debug(f"(hourly) now_weekday = {now_weekday}, self.start_day = {self.start_day}, self.stop_day = {self.stop_day}")

            #start_dt = datetime.datetime(now.year, now.month, now.day, self.start_hour, self.snap_minute)
            if now_weekday >= self.start_day and now_weekday <= self.stop_day:
                # we're within the days they want snaps
                start_dt = datetime.datetime(now.year, now.month, now.day, self.start_hour, self.snap_minute) # datetime of first snap today
                stop_dt = datetime.datetime(now.year, now.month, now.day, self.stop_hour, self.snap_minute)   # datetime of last snap today
                log.debug(f"start_dt = {start_dt}, stop_dt = {stop_dt}")
                if now > start_dt and now < stop_dt:
                    log.debug(f"within hours, now.minute={now.minute}, self.snap_minute={self.snap_minute}")
                    # we're within the hours they want snaps
                    if now.minute < self.snap_minute:
                        # upcoming this hour
                        self.nextsnap_dt = datetime.datetime(now.year, now.month, now.day, now.hour, self.snap_minute)
                    else:
                        # try to sched for next hour
                        temp_date = now + datetime.timedelta(hours=1)
                        temp_date2 = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, temp_date.hour, self.snap_minute)
                        log.debug(f"temp_date2 = {temp_date2}, stop_dt = {stop_dt}")
                        if temp_date2 <= stop_dt:
                            self.nextsnap_dt = temp_date2
                        else:
                            log.debug("outside hours after bump")
                            tomorrow = now + datetime.timedelta(days=1)
                            temp_date2 = datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0,0) # midnight tonight
                            return self.next_snaptime(temp_date2) # recurse - it'll figure it out if we're outside days
                else:
                    # we're within the days, but outside of hours
                    if now < start_dt:
                        log.debug(f"before hours")
                        self.nextsnap_dt = start_dt
                    elif now > start_dt:
                        # after hours - bump forward to next day
                        log.debug(f"after hours")
                        tomorrow = now + datetime.timedelta(days=1)
                        temp_date2 = datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0,0) # midnight tonight
                        return self.next_snaptime(temp_date2) # recurse - it'll figure it out if we're outside days
                    else:
                        self.nextsnap_dt = start_dt
                        return self.nextsnap_dt
            else:
                # we're outside the days specified, go forward to next week
                log.debug(f"outside days")
                day_delta = 7 + self.start_day - now_weekday
                if day_delta > 7:
                    # it's later this week, not next week
                    day_delta -= 7
                temp_date = now + datetime.timedelta(days=day_delta)
                target_datetime = datetime.datetime(temp_date.year, temp_date.month, temp_date.day, self.start_hour, self.snap_minute)
                self.nextsnap_dt = target_datetime

        return self.nextsnap_dt

# - end of new stuff

if __name__ == "__main__":
    log = getLogger()
    log.setLevel(DEBUG)
    #log.setLevel(DEBUG)
    FORMAT = "%(filename)s:%(lineno)s:%(funcName)s():%(message)s"
    # create handler to log to stderr
    console_handler = StreamHandler()
    console_handler.setFormatter(logging.Formatter(FORMAT))
    log.addHandler(console_handler)


    """
    hourlytest = HourlySchedule()

    now = datetime.datetime(2020,12,1,14,22)
    print(f"now is: {now.isoformat()}, next snap for hourly is {hourlytest.next_snaptime(now)}")

    now = datetime.datetime(2020,12,31,14,22)
    print(f"now is: {now.isoformat()}, next snap for hourly is {hourlytest.next_snaptime(now)}")

    now = datetime.datetime(2020,12,31,20,32)
    print(f"now is: {now.isoformat()}, next snap for hourly is {hourlytest.next_snaptime(now)}")
    """
    monthly = MonthlySchedule()
    weekly = WeeklySchedule()
    daily = DailySchedule()
    hourly = HourlySchedule()
    default_sched = SnapSchedule("default", monthly, weekly, daily, hourly)

    monthly = MonthlySchedule(date=2,time=1200)
    weekly = WeeklySchedule(weekday=1,time=1700)
    daily = DailySchedule(time=1100, start_day=0,stop_day=4)
    hourly = HourlySchedule()
    custom_sched = SnapSchedule("custom", monthly, weekly, daily, hourly)

    now = datetime.datetime(2020,12,1,14,22)
    print(f"now is: {now.isoformat()}")
    print(f"next snap for custom_sched is {custom_sched.next_snap(now)}")
    print(f"next snap for default is {default_sched.next_snap(now)}")

    now = datetime.datetime(2020,12,31,14,22)
    print(f"now is: {now.isoformat()}")
    print(f"next snap for custom_sched is {custom_sched.next_snap(now)}")
    print(f"next snap for default is {default_sched.next_snap(now)}")

    now = datetime.datetime(2020,12,31,20,32)
    print(f"now is: {now.isoformat()}")
    print(f"next snap for custom_sched is {custom_sched.next_snap(now)}")
    print(f"next snap for default is {default_sched.next_snap(now)}")



    now = datetime.datetime.now()
    print(f"current date/time is: {now.isoformat()}")
    print(f"next snap for default is {default_sched.next_snap(now)}")

    #custom_sched = SnapSchedule("custom")
    #custom_sched.set_monthly(date=2,time=1200)
    #custom_sched.set_weekly(weekday=1,time=1700)
    #custom_sched.set_daily(time=1100, start_day=0,stop_day=4)
    #custom_sched.set_hourly()

    now = datetime.datetime.now()
    print(f"next snap for custom_sched is {custom_sched.next_snap(now)}")
    #print(custom_sched.next_snap(now))

