## Description
This python package connects to the [datapool program](https://datapool.readthedocs.io/en/latest/#) developed by Eawag & ETH Zurich - SIS.
The package's api (*DataPool class*) represents the [database layout](https://datapool.readthedocs.io/en/latest/_images/DataModel.svg). 
So all "sub-instances" of the DataPool class represents a table in the database layout. To query data from a table use like this:

```python
from datapool_client import DataPool
datapool_instance = DataPool()
datapool_instance.table_name.method()
```

## Requirements
You should have  **Python** (*version 3.9 or greater*) installed on your computer! **Git** is not a requirement, but it is recommended.

## Installation

**Using Git**

Option 1:
```shell 
pip install git+https://gitlab.switch.ch/christian.foerster/datapool_client.git
```
Option 2:
```shell 
git clone https://gitlab.switch.ch/christian.foerster/datapool_client.git
pip install datapool_client
```

**Without Git:**

Download the repository as zip archive and unpack it. Then run:
```shell 
pip install <path-to-unpacked-repo>
```


## Configure default connection to Datapool

In order to work efficiently you might want to provide default connection parameters to the datapool.
That way, you do no need to provide the connection parameters everytime you use this package to access the database.

```python
from datapool_client import set_defaults

# connection parameters (example)
instance = "DEFAULT"
host = "ip.to.datapool.host"
port = "5432"
database = "db_name"
user = "db_user_name"
password = "db_user_password"

# this function sets the default parameters programmatically
set_defaults(
    instance=instance, 
    host=host, 
    port=port, 
    database=database, 
    user=user, 
    password=password
)
```

To overwrite (change) an already made entry you should pass the instance name and the overwrite-keyword!
```python
from datapool_client import set_defaults

instance = "DEFAULT"
new_host = "new.new.new"
new_port = "5432"
new_database = "new"
new_user = "new"
new_password = "new"

set_defaults(
    instance=instance, 
    host=new_host, 
    port=new_port, 
    database=new_database, 
    user=new_user, 
    password=new_password, 
    overwrite=True
)
```

> **Note:** Set multiple defaults by passing **instance** names. Instance refers to a datapool instance, if you have multiple running.


## Usage Examples

```python
# import module
from datapool_client import DataPool

# create instance 
## (with set defaults)
dp = DataPool()

## if you do not have defaults set, you need to provide 
## host, port, user, database and password to connect
## dp = DataPool(
##     host="host",
##     port="port",
##     user="user",
##     database="database",
##     password="password"
## )

'''retrieve data 'low level', via sending sql queries your writing yourself.'''
# run your own specific query, return native python data types
data_dict = dp.query(
    "select * from site;"
)

# run your own specific query, return a pandas dataframe
data_dataframe = dp.query_df(
    "select * from site;"
)

'''some predefined query examples'''
# get the entire source table
my_sources = dp.source.all()

# get the special value definition of a source type
source_type = "your_source_type_name"
dp.special_value_definition.from_source_type(source_type)

# returning source type filtered by source name
source_name = "your_source_name"
dp.source_type.from_source(source_name)

# retrieving the site table
dp.site.all()

# get all signals of source
# input for function must be keyword arguments
site = "your_site_name"
ret_df = dp.signal.get(site_name=site)

source = "your_source_name"
start = "2018-07-11 00:00:00"
end = "2018-07-11 23:55:00"
parameter = "your_parameter_name"
data = dp.signal.get(
    source_name=source, 
    parameter_name=parameter, 
    start=start, 
    end=end
)

'''reshape signal data'''
from datapool_client import reshape
reshaped = reshape(data)

'''retrieve meta data history'''
meta_data = dp.meta_data_history.get(
    source_name="your_source_name"
)

'''plot data with meta_data'''
from datapool_client import Plot
# defaults should be set, or connection details must be provided
pl = Plot()
data, meta_data = pl.plot_signal_with_meta(
    source_name="your_source_name",
)

# ... there's a lot more to discover
```

## Attention

In order to use the package with the whdp you must initialize the DataPool instance like so!

```python
from datapool_client import DataPool

# replace ... with your connection details!!
whdp = DataPool(
    host="...",
    port="5432",
    database="...",
    user="...",
    password="...",
    to_replace={
        "parameter": "variable"
    }
)
```
Ps: There are a few limitations working with this package and the whdp.


## Running Tests

Install pytest and other dependencies.
```shell
pip install -r requirements_dev.txt
```

To run the tests enter the package directory and run:
```shell
pytest
```
