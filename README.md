# mesos-cli
A python based CLI tool to show active frameworks running on Mesos

This script intends to act as an alternative to the standard Mesos UI.
This requires `sshuttle` to be running.
The default target is `odhecx52:5040`. This can be overridden via the `-m` and/or `-p` parameters.

Requires Python 3 and packages in requirements.txt

To install the requirements, run:
```bash
pip install -r requirements.txt
```

## How to use it
```bash
usage: mesos_cli.py [-h] [-a APP | -r APP-REGEX] [-m MASTER] [-p PORT] [-w]

Mesos Master Status Parser

optional arguments:
  -h, --help            show this help message and exit
  -a APP, --app APP     Application Name
  -r REGEX, --regex REGEX     Application Name Regex. Overrides `-a`
  -m MASTER, --master MASTER
                        Mesos Master Host Name
  -p PORT, --port PORT  Port Number
  -w, --watch           Enable Watcher
```

## Examples

```bash
# To view the full list of registered frameworks:
python mesos_cli.py

# To view a single framework by name:
### The name is taken from the list returned via the first call.
python mesos_cli.py -a my_spark_job

# To view a single framework by name matching a regex:
python mesos_cli.py -r 'spark.*job'

# To watch the framework:
### The watch triggers every 5 seconds.
python mesos_cli.py -a my_spark_job -w
```