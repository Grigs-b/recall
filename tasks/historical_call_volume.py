import luigi, numpy, json, cql
from cql.marshal import int64_unpack
from datetime import datetime, timedelta, date, time

class CallHistoryDaily(luigi.Task):
    host = luigi.Parameter()
    port = luigi.IntParameter()
    keyspace = luigi.Parameter()
    day = luigi.DateParameter()
    group = luigi.Parameter()
    
    query = '''SELECT date, value FROM callsoffered2 where group = :group and day = :day'''
    def output(self):
        
        return luigi.LocalTarget('output/calls_offered_%s_%s.tsv' % (self.group, self.day))
        
    def run(self):
        params = dict(day=self.day.isoformat(), group=self.group)
        con = cql.connect(self.host, self.port, self.keyspace, cql_version='3.0.0')
        cursor = con.cursor()
        cursor.execute(self.query, params)
        
        with self.output().open('w') as out:
            for row in cursor:
                db_date = datetime.fromtimestamp(int64_unpack(row[0]) / 1000)
                print >> out, db_date.isoformat(), row[1]
        cursor.close()
        con.close()
        
class CallHistoryLast5WeeksByDay(luigi.Task):
    host = 'ec2-54-211-246-117.compute-1.amazonaws.com'
    port = 9160
    keyspace = 'inovadata'
    group = 'support'
    
    today = date.today()
    interval = timedelta(days=2)
    weeks = 4   #make it N weeks and change to param
    #python is so lovely
    dates = [ (today-(index*interval)) for index in range(weeks) ]
    
    def requires(self):
        return [CallHistoryDaily(self.host, self.port, self.keyspace, date, self.group) for date in self.dates]
    
    def output(self):
        return luigi.LocalTarget('output/calls_offered_5weeks_%s.json' % self.dates[0])
    
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
            print('values keys! %s' % (values.keys()))
            for each_day in values.keys():
                print('day %s, interval keys! %s' % (values[day].keys(), values.keys()))
                try:
                    raw_interval = values[each_day][interval]['raw']
                    #want the count of the number of call events in each interval more than their actual value
                    interval_calls.append(len(raw_interval))
                except KeyError:
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
    
    query = '''SELECT date, value FROM callsoffered2 where group = :group and day > :date_a and day < :date_b'''
    def output(self):
        
        return luigi.LocalTarget('output/calls_offered/%s' % self.date_interval)
        
    def run(self):
        params = { 'date_a' : self.dates.date_a, 'date_b' : self.dates.date_b, 'group' : self.group}
        con = cql.connect(self.host, self.port, self.keyspace, cql_version='3.0.0')
        cursor = con.cursor()
        value = cursor.execute(self.query, params)
        
if __name__ == '__main__':
    luigi.run()
