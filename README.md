# transform_web_traffic.py

This Python script processes log data generated by a web server. It aggregates the time that users spent on each path and generates a pivot table. The output table can be used to analyze which paths are most popular, which users are the most active and many other questions as well.

# Installation
This script requires the use of Python 3.6 or above. There are also several packages which are required that are not included in the standard library. Download the `requirements.txt` file and run the following command to ensure that your environment has the necessary packages installed:

```bash
pip install -r requirements.txt
```
Once your environment is ready, you can download the `transform_web_traffic.py` script and run it in whatever location you choose.

# Input

The input to the script are a set of comma-separated value (CSV) files. These files are stored in S3. All files must have the suffix `.csv`. The CSV files must contain certain fields to be processed successfully.

Field | Data Type | Description
--- | --- | ---
user_id | integer | The unique identifier for the user visiting the page.
path | string | The page within the website that the user visited. The field must be formatted as a slash followed by an identifier and optional additional pairs of slashes and identifiers. For example, `'/features/desktop'` is a valid path.
length | integer | The length of time the user spent on the page in seconds.

Any additional fields in the CSV file will be ignored. These three columns are required for the CSV file to be processed. Any records in the CSV files that have malformed `path` fields or non-integer `length` fields will be ignored.

# Execution

The script has several arguments that control its execution. To see the help section for running the `transform_web_traffic.py` script, use the command:

```bash
python transform_web_traffic.py -h
```

The arguments for the script are:

Argument | Type | Description
--- | --- | ---
bucket | positional (required) | Name of S3 bucket that contains web traffic data
--prefix | optional | Prefix to filter S3 keys (filenames) by
--output | optional | Name of output CSV file. The default value for this variable is `web_traffic.csv`.

As an example, say that we want to process all of the CSV files in the S3 bucket `cauldron-workshop` in the folder `data`. We want the output to go into the file `processed_web_data.csv`. The command we would use to execute this job is:

```bash
python transform_web_traffic.py cauldron-workshop --prefix data --output processed_web_data.csv
```

# Output

The output CSV file is a pivot table with `user_id` as the row label, `path` as the column label and the sum of `length` in each cell. An example of a small output file is:

user_id | /about | /features/desktop | /tutorial/step-four
--- | --- | --- | ---
2 | 0 | 25 |112
3 | 18 | 155 | 31
10 | 95 | 281 | 0

This file can be imported into Excel or other business intelligence tools such as Tableau for further processing.
