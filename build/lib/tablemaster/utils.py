from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def gen_month_list(month_start, month_end):
    l = []
    while datetime.strptime(month_start, '%Y-%m') <= datetime.strptime(month_end, '%Y-%m'):
        l.append(month_start)
        month_start = datetime.strftime(datetime.strptime(month_start, '%Y-%m')+ relativedelta(months=1), '%Y-%m')
    return l

def gen_day_list(day_start, day_end='now'):
    if day_end == 'now':
        day_end = datetime.strftime(datetime.now(), '%Y-%m-%d')
    l = []
    while datetime.strptime(day_start, '%Y-%m-%d') <= datetime.strptime(day_end, '%Y-%m-%d'):
        l.append(day_start)
        day_start = datetime.strftime(datetime.strptime(day_start, '%Y-%m-%d')+ relativedelta(days=1), '%Y-%m-%d')
    return l