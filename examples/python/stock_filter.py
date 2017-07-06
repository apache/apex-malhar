#input_data="apex-malhar/examples/python/resources/stock_data.csv"
input_data="/home/vikram/Documents/src/apex-malhar/examples/python/resources/stock_data.csv"
data = []
with open( input_data, "r") as outfile:
    outfile.readline()
    for line in outfile:
	data.append(line)

def filter_func(a):
  input_data=a.split(",")
  if float(input_data[2])> 30:
     return True
  return False


from pyapex import createApp

def filter_func(a):
  input_data=a.split(",")
  if float(input_data[2])> 30:
     return True
  return False


from pyapex import createApp
a=createApp('python_app').from_data(data) \
  .filter('filter_operator',filter_func) \
  .to_console(name='endConsole') \
  .launch(False)

