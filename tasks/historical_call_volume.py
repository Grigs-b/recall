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
        
        return luigi.LocalTarget('output/calls_offered_%s.tsv' % self.day)
        
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
    today = date.today()
    interval = timedelta(days=7)
    weeks = 5   #make it N weeks and change to param
    #python is so lovely
    dates = [ (today-(index*interval)).isoformat() for index in range(weeks) ]
    dates = [today]
    
    def requires(self):
        return [CallHistoryDaily(date) for date in self.dates]
    
    def output(self):
        return luigi.LocalTarget('output/calls_offered_5weeks_%s.json' % self.dates[-1])
    
    def run(self):
        results = []
        for input in self.input():
            day = ''   
            values = {}
            with input.open('r') as f:
                for line in f:
                    date, value = line.strip().split()
                    metric_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
                    hour = metric_date.hour
                    half_hour = (metric_date.minute // 30) * 30 #gives us either 30 or 0
                    bucket = '{0}:{1}'.format(hour, half_hour)
                    day = metric_date.date().isoformat()
                    try:
                        values[day][bucket]['raw'].append(int(value))
                    except KeyError:
                        values[day] = {}
                        values[day][bucket] = {}
                        values[day][bucket]['raw'] = []
                        values[day][bucket]['raw'].append(int(value))
                        #please dont hate me...
                    
            for bucket in values[day].keys():
                interval = values[day][bucket]['raw']
                values[day][bucket]['mean'] = numpy.mean(interval)
                values[day][bucket]['median'] = numpy.median(interval)
                values[day][bucket]['q1'] = numpy.percentile(interval, 25)
                values[day][bucket]['q3'] = numpy.percentile(interval, 75)
                values[day][bucket]['min'] = min(interval)
                values[day][bucket]['max'] = max(interval)
            results.append(values)
                    
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
