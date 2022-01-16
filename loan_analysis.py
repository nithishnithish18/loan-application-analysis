#Master

import json
conf =json.loads(sc.wholeTextFiles("/FileStore/tables/auto_loan.json").collect()[0][1])
print(conf.get('input_location'))
print(conf.get('output_location'))
print(conf.get('delimiter'))
#function to skip header

def skipheader(index,iterable):
  current_row_num = -1
  for record in iterable:
    current_row_num += 1
    if index == 0 and current_row_num==0:
      continue
    yield record
    
rdd = sc.textFile(conf.get('input_location')).mapPartitionsWithIndex(skipheader)
rdd.cache()


#1. month in which maximum loan request submited in last one year (01-04-2019 to 31-03-2020)

resRDD1 = rdd.filter(lambda x: x.split(",")[5] >= '2019-04-01' and x.split(",")[5] < '2020-04-31') \
          .map(lambda x:(int(x.split(",")[5].split("-")[1]),1)) \
          .reduceByKey(lambda x,y: x+y)

finalRes = resRDD1.reduce(lambda x, y: (x if x[1]>y[1] else y))
print(finalRes)

