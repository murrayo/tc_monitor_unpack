# tc_monitor_unpack
Unpack and chart a grab-bag of interesting metrics from TrakCare Monitor (2016 or later). 

TrakCare Monitor data must be exported using the TrakCare Monitor Tool using "ExportAll".

There is no proprietary information in this code. The basic operation is to read text files and create some charts.

## Create docker container image

- Download the source files
- `cd` to the folder with source files
- Build tc_monitor_unpack container image: `docker build --no-cache -t tc_monitor_unpack .`

## Run the command over a TrakCare monitor extract

Required argument `-d directory` to point to the folder with extracted TrakCare Monitor files.

See the help text:

```plaintext
$ docker run -v "$(pwd)":/data --rm --name tc_monitor_unpack tc_monitor_unpack ./tc_monitor_unpack.py -h

usage: tc_monitor_unpack [-h] -d "/path/path" [-l LISTOFDBS [LISTOFDBS ...]]
                         [-g]

TrakCare Monitor Process

optional arguments:
  -h, --help            show this help message and exit
  -d "/path/path", --directory "/path/path"
                        Directory with Monitor files
  -l LISTOFDBS [LISTOFDBS ...], --listofDBs LISTOFDBS [LISTOFDBS ...]
                        TrakCare databases names to show separately for
                        average episode size
  -g, --exclude_globals
                        Globals metrics take a long time and can be excluded

Be safe, "quote the path"
```

For example, change to the folder up a level from the exported monitor `.txt` files and run the command:

```plaintext
docker run -v "$(pwd)":/data --rm --name tc_monitor_unpack tc_monitor_unpack ./tc_monitor_unpack.py -d "/data/folder with monitor files"
```

Or put the path to the folder with the text files in the docker volume parameter and put the folder name after `-d "/data/folder with monitor files"` 

```plaintext
docker run -v "/path/to/folder/with text files":/data --rm --name tc_monitor_unpack tc_monitor_unpack ./tc_monitor_unpack.py -d /data
```

## Output files

- In the same folder as your input files you will see a set of folders including:
- - `all_out_png` - png charts including summaries of episode counts, jounals, fastest growing databases, database sizes, etc
- - `all_out_csv`, `all_database` etc - If there is something interesting in the charts, look in the other folders created for differently sorted .csv files to create your own charts in excel.


- Also in the same folder as you input files is a summary text file with useful metrics: 
`all_xxxxx_MonitorDatabase_Basic_Stats.txt`. 

## Options

The `-g` flag skips charting of globals metrics. Globals metrics are more concerned with how long components take to run or how many global references are used on average per component. This can take a while and is not so interesting for capacity planning.

If you want to see database growth with/without selected databases in the `all_xxxxx_MonitorDatabase_Basic_Stats.txt` file, you can list databases on the command line to calculate separately.
For example; if you run through once and have a look at the database pie chart. Imagine “PRD-DOCUMENT” is a document database, and you want to see database growth per episode separately for documents. Also imagine "PRD-MONITOR" is the database that collects monitor data. The MONITOR database will purge (for example, after 60 or 90 days) so does not contribute to yearly database growth estimates, so we want to separate that out as well.

Add one or more databases to separate out to the database list on the command line:

```plaintext
docker run -v "/path/to/folder/with text files":/data --rm --name tc_monitor_unpack tc_monitor_unpack ./tc_monitor_unpack.py -d /data -l PRD_DOCUMENT PRD-MONITOR
```

# Updates

Remove the old image and create a new one with updated source code

`docker rmi tc_monitor_unpack`

