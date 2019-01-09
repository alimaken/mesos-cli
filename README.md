# mesos-cli
A python based CLI tool to show active frameworks running on Mesos

This script intends to act as an alternative to the standard Mesos UI.
This requires `sshuttle` to be running.
The default target is `odhecx52:5040`. This can be overridden via the `-m` and/or `-p` parameters.

## How to use it
```bash
usage: mesosython.py [-h] [-H HELP] [-a APP] [-m MASTER] [-p PORT] [-w]

Mesos Master Status Parser

optional arguments:
  -h, --help            show this help message and exit
  -H HELP, --Help HELP  Example: Help argument
  -a APP, --app APP     Application Name
  -m MASTER, --master MASTER
                        Mesos Master Host Name
  -p PORT, --port PORT  Port Number
  -w, --watch           Enable Watcher
```

## Examples

```bash
# To view the full list of registered frameworks:
python mesosython.py

# To view a single framework by name:
### The name is taken from the list returned via the first call.
python mesosython.py -a my_spark_job

# To watch the framework:
### The watch triggers every 5 seconds.
python mesosython.py -a my_spark_job -w
```