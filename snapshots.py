# rewritten for new scheduling - Bruce Clagett
# 6/2021
#

import logging
import calendar
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY, DAILY, MINUTELY

log = logging.getLogger(__name__)

days_abbr_names = list(calendar.day_abbr)
days_long_names = list(calendar.day_name)
days_all_names = days_abbr_names + days_long_names
months_abbr_names = list(calendar.month_abbr)
months_long_names = list(calendar.month_name)
months_abbr_names.remove('')
months_long_names.remove('')
months_all_names = months_abbr_names + months_long_names

def is_everyday(every):
    return every.lower() == "day"

def is_everymonth(every):
    return every.lower() == "month"

def comma_string_to_list(comma_string):
    # Takes a string representing a comma separated list
    # such as "Mon, Tue,We"
    # removes all spaces, then splits string at commas into list
    return comma_string.replace(" ", "").split(",")

def is_day_list(every):
    for day_name in comma_string_to_list(every):
        if day_name.capitalize() not in days_all_names:
            return False
    return True

def is_month_list(every):
    for month_name in comma_string_to_list(every):
        if month_name.capitalize() not in months_all_names:
            return False
    return True

def day_to_num(day):
    day = day.capitalize()
    if day in days_abbr_names:
        d = days_abbr_names.index(day)
    elif day in days_long_names:
        d = days_long_names.index(day)
    else:
        return -1
    return d

def month_to_num(month):
    month = month.capitalize()
    if month in months_abbr_names:
        d = months_abbr_names.index(month) + 1
    elif month in months_long_names:
        d = months_long_names.index(month) + 1
    else:
        return -1
    return d

def _parse_spec_int(str_num, spec_name, name, min_allowed, max_allowed):
    try:
        result = int(str_num)
    except Exception as exc:
        log.error(f"Invalid {spec_name}: {str_num} for schedule {name}: {exc}")
        raise
    if result < min_allowed or result > max_allowed:
        log.error(f"Integer out of range: {result} for {spec_name} " +
                  f"in schedule {name} should be in the range [{min_allowed}-{max_allowed}]")
        log.error(f"Defaulting to {min_allowed}")
        return min_allowed
    return result

def _parse_interval(interval, name):
    result = _parse_spec_int(interval, 'interval', name, 1, 1439)
    return result

def _parse_retain(retain, name):
    result = _parse_spec_int(retain, 'retain', name, 0, 99)
    return result

def _parse_day_of_month(dom, name):
    result = _parse_spec_int(dom, 'day_of_month', name, 1, 31)
    return result

def _parse_upload(upload, name):
    if str(upload).lower() in ["yes", "true", "1"]:
        return True
    elif str(upload).lower() in ["no", "false", "0"]:
        return False
    else:
        log.error(f"Invalid upload specification, should be yes/no/true/false: {upload} for schedule {name}")
        log.error(f"Defaulting to false.")
        return False

def _parse_time(at, name="Unknown"):
    ignored_date = "20200101 "
    try:
        result = parser.parse(ignored_date + at)
    except parser.ParserError as exc:
        errmsg = f"Invalid time spec: '{at}' for schedule {name}: {exc}"
        log.error(errmsg)
        raise
    return result

def _parse_days(everyspec):
    if is_everyday(everyspec):
        return [0, 1, 2, 3, 4, 5, 6]
    l_spec = comma_string_to_list(everyspec)
    result = list(map(day_to_num, l_spec))
    result.sort()
    if result[0] < 0:
        log.error(f"Error parsing days: {everyspec}; defaulting to every day")
        return [0, 1, 2, 3, 4, 5, 6]
    return result

def _parse_months(everyspec):
    if is_everymonth(everyspec):
        return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    everyspec = comma_string_to_list(everyspec)
    result = list(map(month_to_num, everyspec))
    result.sort()
    if result[0] < 1:
        log.error(f"Error parsing months: {everyspec}; defaulting to every month")
        return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    return result

def _parse_every(every):
    if is_everyday(every) or is_day_list(every):
        return 'day', _parse_days(every)
    if is_everymonth(every) or is_month_list(every):
        return 'month', _parse_months(every)
    return 'error', None


def _parse_schedule_spec(sched_spec, name):
    every_type, every, at, until, interval = None, None, None, None, None
    retain = 4
    day = 1
    upload = False
    if 'every' in sched_spec:
        every_type, every = _parse_every(sched_spec['every'])
    if 'retain' in sched_spec:
        retain = _parse_retain(sched_spec['retain'], name)
    if 'at' in sched_spec:
        at = _parse_time(sched_spec['at'], name)
    if 'interval' in sched_spec:
        interval = _parse_interval(sched_spec['interval'], name)
    if 'until' in sched_spec:
        until = _parse_time(sched_spec['until'], name)
    if 'upload' in sched_spec:
        upload = _parse_upload(sched_spec['upload'], name)
    if 'day' in sched_spec:
        day = _parse_day_of_month(sched_spec['day'], name)
    if not at:
        at = _parse_time("0000", name)
    if interval and not until:
        until = _parse_time("2359", name)
    return every_type, every, retain, at, interval, until, upload, day

def parse_schedule_entry(schedule_groupname, schedule_name, sched_spec):
    name = schedule_name
    if schedule_groupname:
        name = schedule_groupname + "_" + schedule_name
    log.debug(f"{name}\t{sched_spec}")
    if len(name) > 19:
        log.error(f"While parsing config file: for {name} (len={len(name)}):")
        log.error(f"   Length of schedule group name + name must be less than 18 characters")
        log.error(f"   Ignoring entry.")
        return None
    every_type, every, retain, at, interval, until, upload, day = \
        _parse_schedule_spec(sched_spec, name)
    if every_type == 'month':
        entry = MonthlyScheduleEntry(name, every, retain, at, day, upload)
    elif every_type == 'day' and interval:
        entry = IntervalScheduleEntry(name, every, retain, at, until, interval, upload)
    elif every_type == 'day':
        entry = DailyScheduleEntry(name, every, retain, at, upload)
    else:  # error
        log.error(f"Invalid 'every:' spec - schedule: {name}, every: {every}")
        log.error(f"   Ignoring entry.")
        return None
    entry.groupname = (schedule_groupname or schedule_name)
    return entry


class _BaseScheduleEntry(object):
    def __str__(self):
        time = str(self.at.hour).zfill(2) + str(self.at.minute).zfill(2)
        return f"{self.name}:at={time}:retain={self.retain}:upload={self.upload}"

    def __init__(self, name, retain, at, upload=False, sort_priority=9999):
        self.groupname = None
        self.name = name
        self.retain = retain
        self.upload = upload
        self.nextsnap_dt = datetime.min
        self.at = at
        self.no_upload = not upload    # for secondary sorting
        self.sort_priority = sort_priority      # for 3rd level sort
        log.debug(f'Init Schedule Entry: {str(self)}')

    def calc_next_snaptime(self, now):
        # should never be called
        return datetime.min

class MonthlyScheduleEntry(_BaseScheduleEntry):
    def __str__(self):
        return "Monthly:" + _BaseScheduleEntry.__str__(self) + f":months={self.month_list}:day={self.day} "

    def __init__(self, name, month_list, retain, at, day, upload, sort_priority=10):
        self.day = day
        self.month_list = month_list
        _BaseScheduleEntry.__init__(self, name, retain, at, upload, sort_priority)

    def calc_next_snaptime(self, now):
        log.debug(f" (monthly) now={now}, self.nextsnap_dt={self.nextsnap_dt}")
        if self.retain == 0:    # force sort to the end of time
            log.warning(f"Snapshot {self.name} has retain=0")
            self.nextsnap_dt = datetime.max
            return self.nextsnap_dt
        now = now.replace(second=0, microsecond=0)
        if self.nextsnap_dt < now:
            log.debug(f" computing new monthly target...")
            # use first of month because rrule skips missing dates like feb 31 and we want those months
            target_datetime = datetime(now.year, now.month, 1, self.at.hour, self.at.minute)
            log.debug(f"target_datetime: {target_datetime}")
            candidates = list(rrule(MONTHLY, dtstart=target_datetime, bymonth=self.month_list, count=2))
            # set day via relative delta to properly handle end of month cases
            day_rel = relativedelta(day=self.day)
            log.debug(f"     candidates: {candidates}")
            candidates = [c+day_rel for c in candidates]
            if candidates[0] < now:
                self.nextsnap_dt = candidates[1]
            else:
                self.nextsnap_dt = candidates[0]
        log.debug(f"    returning self.nextsnap_dt={self.nextsnap_dt}")
        return self.nextsnap_dt


class DailyScheduleEntry(_BaseScheduleEntry):
    def __str__(self):
        return "Daily:" + _BaseScheduleEntry.__str__(self) + f":days={self.weekday_list}"

    def __init__(self, name, weekday_list, retain, at, upload, sort_priority=50):
        self.weekday_list = weekday_list
        _BaseScheduleEntry.__init__(self, name, retain, at, upload, sort_priority)

    def find_next_daily_snaptime(self, now, hour, minute):
        target_datetime = datetime(now.year, now.month, now.day, hour, minute)
        candidates = list(rrule(DAILY, dtstart=target_datetime, byweekday=self.weekday_list, count=2))
        log.debug(f"     candidates: {candidates}")
        if candidates[0] < now:
            return candidates[1]
        else:
            return candidates[0]

    def calc_next_snaptime(self, now):
        log.debug(f" (daily) now={now}, self.nextsnap_dt={self.nextsnap_dt}")
        if self.retain == 0:    # force sort to the end of time
            log.warning(f"Snapshot {self.name} has retain=0")
            self.nextsnap_dt = datetime.max
            return self.nextsnap_dt
        now = now.replace(second=0, microsecond=0)
        if self.nextsnap_dt < now:
            log.debug(f"(daily), now > next snap")
            self.nextsnap_dt = self.find_next_daily_snaptime(now, self.at.hour, self.at.minute)
        log.debug(f"(daily), returning self.nextsnap_dt={self.nextsnap_dt}")
        return self.nextsnap_dt


class IntervalScheduleEntry(DailyScheduleEntry):
    def __str__(self):
        h = self.until.hour
        m = self.until.minute
        time = str(h).zfill(2) + str(m).zfill(2)
        return "Interval-" + DailyScheduleEntry.__str__(self) + f":interval={self.interval}:until={time}"

    def __init__(self, name, weekday_list, retain, at, until, interval, upload):
        self.interval = interval
        self.until = until
        sort_priority = 1440 + 100 - interval   # Daily always wins for intervals < 1 day
        DailyScheduleEntry.__init__(self, name, weekday_list, retain, at, upload, sort_priority)

    def calc_next_snaptime(self, now):
        # get rid of seconds
        now = now.replace(second=0, microsecond=0)
        if self.retain == 0:
            log.warning(f"Snapshot {self.name} has retain=0")
            self.nextsnap_dt = datetime.max
            return self.nextsnap_dt
        # find next valid date containing self.until
        until_dt = self.find_next_daily_snaptime(now, self.until.hour, self.until.minute)
        start = datetime(until_dt.year, until_dt.month, until_dt.day, self.at.hour, self.at.minute)
        log.debug(f"(Interval) now:{now} start:{start} until:{until_dt}")
        if now <= start:  # no need to list candidates
            self.nextsnap_dt = start
        else:
            # find the candidate that is within <interval> minutes of now
            # calculation is based on start, so easiest method is generate all times between start and now + interval
            # rrule seems pretty optimized and plenty fast enough given size of problem.
            max_from_now = min(until_dt, now + relativedelta(minutes=+self.interval - 1))
            log.debug(f"(Interval) max_from_now: {max_from_now}")
            candidates = list(rrule(MINUTELY, dtstart=start, interval=self.interval, until=max_from_now))
            candidate = candidates[-1]
            if candidate < now:  # last candidate for today already passed, go to next day's start
                candidate = self.find_next_daily_snaptime(start + relativedelta(days=+1), start.hour, start.minute)
            self.nextsnap_dt = candidate
        return self.nextsnap_dt

def _test_result_message(msg, always_print=False):
    if always_print:
        print('   ', msg)
    else:
        log.debug(msg)

def _run_schedule_test(test_name, entry, test_time, expected):
    always_print = (__name__ == '__main__')
    _test_result_message(f"test: {test_name}, schedule: {entry}", always_print)
    _test_result_message(f"      now: {test_time} -- expected: {expected}", always_print)
    result = entry.calc_next_snaptime(test_time)
    success = "FAILED !!!!"
    if str(result) == expected:
        success = "ok"
        _test_result_message(f"{'':<31}-- nextsnap: {result}   --   {success}", always_print)
    else:
        _test_result_message(f"{success} {'':*<25} -- nextsnap: {result}   --   {success}", always_print)
        log.error(f"Self test {test_name} FAILED.  Check debug logs.")


def run_schedule_tests():
    log.info(f"Snapshots schedule tests starting")
    entry = MonthlyScheduleEntry("M-Jan-2-8am", _parse_months('Jan'), 5, _parse_time("8am"), 2, False)
    _run_schedule_test("m01", entry, datetime(2021, 6, 23, 15, 30, 59), "2022-01-02 08:00:00")
    entry = MonthlyScheduleEntry("M-Feb-31-9:05am", _parse_months('Feb'), 5, _parse_time("9:05am"), 31, False)
    _run_schedule_test("m02", entry, datetime(2021, 6, 23, 15, 30, 59), "2022-02-28 09:05:00")
    entry = MonthlyScheduleEntry("M-JunJulAug-31-7am", _parse_months('Jun,Jul,Aug'), 5, _parse_time("7am"), 31, False)
    _run_schedule_test("m03", entry, datetime(2021, 6, 23, 15, 30, 59), "2021-06-30 07:00:00")
    _run_schedule_test("m04", entry, datetime(2021, 6, 30, 15, 30, 59), "2021-07-31 07:00:00")
    entry = MonthlyScheduleEntry("M-everymonth-31-7am", _parse_months('month'), 5, _parse_time("7am"), 31, False)
    _run_schedule_test("m05", entry, datetime(2021, 6, 23, 15, 30, 59), "2021-06-30 07:00:00")
    _run_schedule_test("m06", entry, datetime(2021, 6, 30, 7, 0, 59), "2021-06-30 07:00:00")
    _run_schedule_test("m07", entry, datetime(2021, 6, 30, 7, 1, 59), "2021-07-31 07:00:00")
    _run_schedule_test("m08", entry, datetime(2021, 12, 31, 7, 0, 59), "2021-12-31 07:00:00")
    _run_schedule_test("m09", entry, datetime(2021, 12, 31, 7, 1, 59), "2022-01-31 07:00:00")
    entry = MonthlyScheduleEntry("M-every3-31-7am", _parse_months('Jan,Apr,Jul,Oct'), 5, _parse_time("2am"), 31, False)
    _run_schedule_test("m05", entry, datetime(2021, 2, 23, 0, 30, 59), "2021-04-30 02:00:00")
    _run_schedule_test("m06", entry, datetime(2021, 6, 30, 7, 0, 59), "2021-07-31 02:00:00")
    entry = MonthlyScheduleEntry("M-Jun-23-7am", _parse_months('Jun'), 5, _parse_time("7am"), 23, False)
    _run_schedule_test("m10", entry, datetime(2021, 12, 31, 7, 1, 59), "2022-06-23 07:00:00")
    # entry = MonthlyScheduleEntry("M-Jun-23-7am-bad-time", parse_months('Jan'), 5, parse_time("bad_time"), 23, False)

    entry = DailyScheduleEntry("D-Mon-9am", _parse_days('Mon'), 4, _parse_time("9am"), False)
    _run_schedule_test("d01", entry, datetime(2021, 5, 29, 3, 15, 59), "2021-05-31 09:00:00")
    _run_schedule_test("d02", entry, datetime(2021, 5, 31, 21, 5, 59), "2021-06-07 09:00:00")

    entry = IntervalScheduleEntry("I-MonWed-0903-1700-10min", _parse_days('Mon,Wed'), 4,
                                  _parse_time("9:03am"), _parse_time("5pm"), 10, False)
    _run_schedule_test("i03", entry, datetime(2021, 5, 31, 21, 5, 59), "2021-06-02 09:03:00")
    _run_schedule_test("i04", entry, datetime(2021, 6, 2, 9, 4, 59), "2021-06-02 09:13:00")
    _run_schedule_test("i05", entry, datetime(2021, 6, 2, 9, 12, 31), "2021-06-02 09:13:00")
    _run_schedule_test("i06", entry, datetime(2021, 6, 2, 9, 13, 31), "2021-06-02 09:13:00")
    _run_schedule_test("i07", entry, datetime(2021, 6, 2, 16, 42, 31), "2021-06-02 16:43:00")
    _run_schedule_test("i08", entry, datetime(2021, 6, 2, 16, 54, 31), "2021-06-07 09:03:00")
    _run_schedule_test("i09", entry, datetime(2021, 6, 3, 16, 54, 31), "2021-06-07 09:03:00")

    entry = IntervalScheduleEntry("I-MonWed-0903-1700-1min", _parse_days('Mon,Wed'), 4,
                                  _parse_time("9:03am"), _parse_time("5pm"), 1, False)
    _run_schedule_test("i11", entry, datetime(2021, 6, 3, 16, 54, 31), "2021-06-07 09:03:00")
    _run_schedule_test("i12", entry, datetime(2021, 6, 2, 16, 54, 31), "2021-06-02 16:54:00")
    _run_schedule_test("i13", entry, datetime(2021, 6, 3, 16, 54, 31), "2021-06-07 09:03:00")
    _run_schedule_test("i14", entry, datetime(2021, 6, 2, 17, 0, 31), "2021-06-02 17:00:00")
    entry = IntervalScheduleEntry("I-Mon-Fri-0905-1700-1min", _parse_days('Mon,Tue,Wed,Thu,Fri'), 4,
                                  _parse_time("9:05am"), _parse_time("5pm"), 60, False)
    _run_schedule_test("i15", entry, datetime(2021, 6, 29, 11, 6, 31), "2021-06-29 12:05:00")
    entry = IntervalScheduleEntry("I-Mon-Fri-0905-1700-1min", _parse_days('Mon,Tue,Wed,Thu,Fri'), 4,
                                  _parse_time("9:05am"), _parse_time("5pm"), 5, False)
    _run_schedule_test("i16", entry, datetime(2021, 6, 29, 11, 5, 59), "2021-06-29 11:05:00")
    _run_schedule_test("i17", entry, datetime(2021, 6, 29, 11, 5, 30), "2021-06-29 11:05:00")
    _run_schedule_test("i18", entry, datetime(2021, 6, 29, 11, 5, 1), "2021-06-29 11:05:00")
    _run_schedule_test("i19-microsecond-check", entry, datetime(2021, 6, 29, 11, 5, 59, 999999), "2021-06-29 11:05:00")
    if __name__ == "__main":   # run intentional failure to check output/logging
        _run_schedule_test("i20-fail-INTENTIONAL", entry, datetime(2021, 6, 29, 11, 5, 59), "2021-06-29 11:05:01")

    entry = IntervalScheduleEntry("I-everyday-0903-1700-1min", _parse_days('day'), 4, _parse_time("0000"),
                                  _parse_time("2359"), 1, False)
    _run_schedule_test("i10-now-test", entry, datetime.now(), str(datetime.now() + relativedelta(second=0, microsecond=0)))
    log.info(f"Snapshots schedule tests complete")


if __name__ == "__main__":
    filler = f"{'':-<35}"
    print(f"\n\n{filler}   Running main in snapshots directly   {filler}\n\n")
    log.setLevel(logging.INFO)
    FORMAT = "%(asctime)s:%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s(): %(message)s"
    # create handler to log to stderr
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(FORMAT))
    log.addHandler(console_handler)
    run_schedule_tests()
    print("\n")
