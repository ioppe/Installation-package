"""RunPy

Usage:
 run.py <runOption> <runType> <dataType> <rund> <num>
 run.py (-h | --help)
 run.py --version

Options:
 -h --help      Show this screen.
 --version      Show version
 <rund>
 <num>
 <runOption>    the way to run spark job. such as standalone,yarn or k8s 
 <runType>      use docker or bare, k8s only have docker
 <dataType>     the dataType and dataSize such as wordcount-100g,terasort-20g
"""

from docopt import docopt 
import re
import os
import sys
import math

filepath=""
def runSparkJob(arg):
 global filepath
 runOption = arg['<runOption>'].lower()
 if not arg['<runType>'] == None:
   runType = arg['<runType>'].lower()
 else:
   runType = ""
 dataType = arg['<dataType>'].lower()
 rund = arg['<rund>'].lower()
 num = arg['<num>']
 os.system('mkdir -p ' + '/home/compare/logs/' + dataType + '-' + runOption + '-' + runType + '-' + rund)
 filepath='/home/compare/logs/' + dataType + '-' + runOption + '-' + runType + '-' + rund + '/config' + str(num) + '-report.log'
 #运行
 Option(runOption,runType,dataType)
 #检查时间
 Check(rund,runOption,runType,dataType,num)
 

def Option(runOption,runType,dataType):
 if runOption == "standalone":
  standalone().run(runType,dataType)
 elif runOption == "yarn":
  yarn().run(runType,dataType)
 elif runOption == "k8s":
  if runType == "bare":
    print("the runType has error! k8s only have docker")
    exit(1)
  k8s().run(dataType)
 else:
  print("the runOption has error! please use standalone,yarn or k8s")
  exit(1)

def Check(rund,runOption,runType,dataType,num):
  time=""
  
  try:
    for line in open(filepath):
      if not line.find("took") == -1:
        if not line.find("finished") == -1:
            spendtime = line.split(" ")
            time=time+spendtime[len(spendtime)-2]+" "
        else:
            time=""
    if not time == "":
      os.system('echo ' + time + ' >> /home/collect/runtime/' + dataType + '-' + runOption + '-' + runType + '-' + rund + '.runtime 2>&1')
    else:
      os.system('echo false >> /home/collect/runtime/' + dataType + '-' + runOption + '-' + runType + '-' + rund + '.runtime 2>&1')
  except IOError:
    print("no log file")
    exit(1)

class standalone():
 def run(self,runType,dataType):
  if runType == "docker":
   self.docker(dataType)
  elif runType == "bare":
   self.bare(dataType)
  else:
   print("the runType has error! standalone only have docker and bare")
   exit(1)

 def docker(self, dataType):
  dataType=dataType.split("-")
  dataSize=dataType[1]
  dataType=dataType[0]
  if dataType == "wordcount":
   self.docker_wordcount(dataSize)
  elif dataType == "terasort":
   self.docker_terasort(dataSize)
  else:
    print("only have wordcount and terasort")
    exit(1)

 def bare(self,dataType):
  dataType=dataType.split("-")
  dataSize=dataType[1]
  dataType=dataType[0]
  if dataType == "wordcount":
   self.bare_wordcount(dataSize)
  elif dataType == "terasort":
   self.bare_terasort(dataSize)
  else:
    print("only have wordcount and terasort")
    exit(1)

 def docker_wordcount(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3))
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4))
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && docker exec spark-driver bin/spark-submit --master spark://172.22.0.2:7077 --deploy-mode client --properties-file /opt/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Input hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > %s 2>&1" % (size,size,filepath))

 def docker_terasort(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3)/100)
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4)/100)
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && docker exec spark-driver bin/spark-submit --master spark://172.22.0.2:7077 --deploy-mode client --properties-file /opt/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaTeraSort /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Input hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Output > %s 2>&1" % (size,size,filepath))
  

 def bare_wordcount(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3))
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4))
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && source /etc/profile && spark-submit --master spark://k8s-node04:7077 --deploy-mode client --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Input hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > %s 2>&1" % (size,size,filepath))

 def bare_terasort(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3)/100)
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4)/100)
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && source /etc/profile && spark-submit --master spark://k8s-node04:7077 --deploy-mode client --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaTeraSort /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Input hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Output > %s 2>&1" % (size,size,filepath))

class yarn():
 def run(self,runType,dataType):
  if runType == "docker":
   self.docker(dataType)
  elif runType == "bare":
   self.bare(dataType)
  else:
   print("the runType has error! yarn only have bare and docker")
   exit(1)

 def docker(self, dataType):
  dataType=dataType.split("-")
  dataSize=dataType[1]
  dataType=dataType[0]
  if dataType == "wordcount":
   self.docker_wordcount(dataSize)
  elif dataType == "terasort":
   self.docker_terasort(dataSize)
  else:
    print("only have wordcount and terasort")
    exit(1)

 def bare(self,dataType):
  dataType=dataType.split("-")
  dataSize=dataType[1]
  dataType=dataType[0]
  if dataType == "wordcount":
   self.bare_wordcount(dataSize)
  elif dataType == "terasort":
   self.bare_terasort(dataSize)
  else:
    print("only have wordcount and terasort")
    exit(1)

 def docker_wordcount(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3))
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4))
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && export MOUNTS=\"/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro\"&& export DOCKER_CLIENT_CONFIG=/home/compare/config.json && su - yarn -c \"export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master yarn --properties-file $SPARK_HOME/conf/spark-defaults.conf --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=local/spark:v1 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG=$DOCKER_CLIENT_CONFIG --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=local/spark:v1 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG=$DOCKER_CLIENT_CONFIG --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS --class com.intel.hibench.sparkbench.micro.ScalaWordCount /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Input hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output\" > %s 2>&1" % (size,size,filepath))

 def docker_terasort(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3)/100)
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4)/100)
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && export MOUNTS=\"/etc/passwd:/etc/passwd:ro,/etc/group:/etc/group:ro\"&& export DOCKER_CLIENT_CONFIG=/home/compare/config.json && su - yarn -c \"export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master yarn --properties-file $SPARK_HOME/conf/spark-defaults.conf --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_TYPE=docker --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=local/spark:v1 --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG=$DOCKER_CLIENT_CONFIG --conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_TYPE=docker --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=local/spark:v1 --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_CLIENT_CONFIG=$DOCKER_CLIENT_CONFIG --conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS=$MOUNTS --class com.intel.hibench.sparkbench.micro.ScalaTeraSort /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Input hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Output\" > %s 2>&1 " % (size,size,filepath))
  

 def bare_wordcount(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3))
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4))
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && su - yarn -c \"export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master yarn --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Input hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output\" > %s 2>&1 " % (size,size,filepath))

 def bare_terasort(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3)/100)
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4)/100)
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && su - yarn -c \"export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master yarn --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaTeraSort /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Input hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Output\" > %s 2>&1 " % (size,size,filepath))

class k8s():
 def run(self,dataType):
  dataType=dataType.split("-")
  dataSize=dataType[1]
  dataType=dataType[0]
  if dataType == "wordcount":
   self.k8s_wordcount(dataSize)
  elif dataType == "terasort":
   self.k8s_terasort(dataSize)
  else:
    print("only have wordcount and terasort")
    exit(1)

 def k8s_wordcount(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3))
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4))
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master k8s://https://k8s-node04:6443 --conf spark.kubernetes.container.image=192.168.0.40/library/spark:v2.4.4 --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaWordCount /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Input hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > %s 2>&1 " % (size,size,filepath))

 def k8s_terasort(self,dataSize):
  dataSize=re.findall(r'[A-Za-z]+|\d+', dataSize)
  dataUnit=dataSize[1].lower()
  dataSize=int(dataSize[0])
  size=0
  if dataUnit == "g":
   size=int(dataSize*math.pow(1024,3)/100)
  elif dataUnit == "t":
   size=int(dataSize*math.pow(1024,4)/100)
  else:
   print("the dataSize has error!")
   exit(1)
  os.system("source /etc/profile && hdfs dfs -rm -r hdfs://192.168.0.40:9000/HiBench/Wordcount/%s/Output > /dev/null 2>&1" % (size))
  os.system("source /etc/profile && export SPARKBENCH_PROPERTIES_FILES=/usr/local/home/hibench/hibench/report/wordcount/spark/conf/sparkbench/sparkbench-template.conf && $SPARK_HOME/bin/spark-submit --master k8s://https://k8s-node04:6443 --conf spark.kubernetes.container.image=192.168.0.40/library/spark:v2.4.4 --properties-file $SPARK_HOME/conf/spark-defaults.conf --class com.intel.hibench.sparkbench.micro.ScalaTeraSort /usr/local/home/hibench/hibench/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Input hdfs://192.168.0.40:9000/HiBench/Terasort/%s/Output > %s 2>&1" % (size,size,filepath))

if __name__ == "__main__":
    arguments = docopt(__doc__, version='1.0.0')
    runSparkJob(arguments)
