import luigi, numpy, json
from cassandra.cluster import Cluster
from cassandra.marshal import int64_unpack
from datetime import datetime, timedelta, date, time

class CallHistoryDaily(luigi.Task):
    orgid = luigi.IntParameter()
    host = luigi.Parameter()
    port = luigi.IntParameter()
    keyspace = luigi.Parameter()
    day = luigi.DateParameter()
    group = luigi.Parameter()
    
    query = '''SELECT datestamp, value FROM skillmetric where orgid = %(orgid)s and metricname = "Calls Offered" and skillname = %(group)s and datestamp > %(date_begin)s and datestamp < %(date_end)s'''
    def output(self):
        
        return luigi.LocalTarget('output/calls_offered_%s_%s.tsv' % (self.group, self.day))

    def get_start_end_times(self):
        #format: 2014-03-17
        begin = datetime.strptime(self.day, '%Y-%m-%d')
        end = begin + timedelta(days=1)
        return begin, end

    def run(self):
        date_begin, date_end = self.get_start_end_times()
        params = dict(orgid=self.orgid,
                      date_begin=date_begin.isoformat(),
                      date_end=date_end.isoformat(),
                      group=self.group)
        cluster = Cluster([self.host], self.port)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)
        ret = session.execute(self.query, params)
        
        with self.output().open('w') as out:
            for row in ret:
                db_date = datetime.utcfromtimestamp(int64_unpack(row[0]) / 1000)
                print >> out, db_date.isoformat(), int(row[1])
        session.shutdown()
        cluster.shutdown()
        
class CallHistoryLastNWeeksByDay(luigi.Task):
    host = luigi.Parameter()
    port = 9042
    keyspace = 'inovadata'
    orgid = luigi.IntParameter()
    group = luigi.Parameter()
    
    today = date.today()
    interval = timedelta(days=7)
    weeks = 6
    #python is so lovely
    #don't get today, do you want zeroes? cause thats how you get zeroes
    dates = [(today-((index+1)*interval)) for index in range(weeks)]
    
    def requires(self):
        return [CallHistoryDaily(self.orgid, self.host, self.port, self.keyspace, date, self.group) for date in self.dates]
    
    def output(self):
        return luigi.LocalTarget('output/calls_offered_6_weeks_%s.json' % self.group.replace(' ', ''))
    
    def run(self):
        results = {}
        values = {}
        for input in self.input():
            day = ''   
            
            with input.open('r') as f:
                for line in f:
                    date, value = line.strip().split()
                    metric_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
                    day = metric_date.date().isoformat()
                    if day not in values.keys():
                        values[day] = {}
                    
                    interval_hour = metric_date.hour
                    half_hour = (metric_date.minute // 30) * 30 #gives us either 30 or 0
                    t = time(hour=interval_hour, minute=half_hour)
                    bucket = t.strftime('%H:%M')
                    
                    try:
                        values[day][bucket]['raw'].append(int(value))
                    except KeyError:
                        values[day][bucket] = {}
                        values[day][bucket]['raw'] = []
                        values[day][bucket]['raw'].append(int(value))
                        #please dont hate me...
        

            for interval in values[day].keys():
                interval_calls = []
                results[interval] = {}

                for each_day in values.keys():
                    #print('day %s, interval keys! %s' % (values[day].keys(), values.keys()))
                    try:
                        raw_interval = values[each_day][interval]['raw']
                        print('raw interval: [%s/%s]:  %s' % (each_day, interval, len(raw_interval)))
                        #want the count of the number of call events in each interval more than their actual value
                        interval_calls.append(len(raw_interval))
                    except KeyError:
                        print('KeyError on interval [%s / %s]' % (each_day, interval))
                        interval_calls.append(0)
                #need to add the interval as a value because we will need each key as an array
                results[interval]['interval'] = interval
                results[interval]['mean'] = numpy.mean(interval_calls)
                results[interval]['median'] = numpy.median(interval_calls)
                results[interval]['q1'] = numpy.percentile(interval_calls, 25)
                results[interval]['q3'] = numpy.percentile(interval_calls, 75)
                results[interval]['min'] = min(interval_calls)
                results[interval]['max'] = max(interval_calls)
            
                    
        with self.output().open('w') as out:
            out.write(json.dumps(results))
            
class CallHistoryInterval(luigi.Task):
    host = luigi.Parameter()
    port = luigi.Parameter()
    keyspace = luigi.Parameter()
    dates = luigi.DateIntervalParameter()
    group = luigi.Parameter()
    
    query = '''SELECT date, value FROM callsoffered where group = :group and date > :date_a and date < :date_b'''
    def output(self):
        
        return luigi.LocalTarget('output/calls_offered/%s' % self.date_interval)
        
    def run(self):
        params = { 'date_a' : self.dates.date_a, 'date_b' : self.dates.date_b, 'group' : self.group}
        cluster = Cluster([self.host], self.port)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)
        ret = session.execute(self.query, params)
        session.shutdown()
        cluster.shutdown()
        
if __name__ == '__main__':
    luigi.run()
