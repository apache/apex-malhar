#Developing Streaming Application in Python#

Currently we have exposed basic support for Stateless Support.

##Requirements:##
* Python 2.7
* py4j
  Please install py4j on your machine.
  ```
  pip install py4j
  ```


Once you have pulled Apache Malhar project, go to python project and follow next steps:

* Compile all projects under Apache Malhar and make sure you have hadoop installed on local node.
* Once compilation finish, go to python/script directory and launch ./pyshell
* This will launch python shell. Now you can develop your application using python shell.

You can write simpler application using available apis as well provide custom functions written in python.

```

def filter_function(a):
  input_data=a.split(',')
  if float(input_data[2])> 100:
     return True
  return False

from pyapex import createApp
a=createApp('python_app').from_kafka09('localhost:2181','test_topic') \
  .filter('filter_operator',filter_function) \
  .to_console(name='endConsole') \
  .launch(False)
```


Note: Currently developer need to ensure that required python dependencies are installed on Hadoop cluster.  
