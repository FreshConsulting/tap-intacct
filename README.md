# tap-intacct
Tap for [Intacct](https://www.sageintacct.com/). This tap does not interact with the Intacct API, it relies on Intaccts Data Delivery Service to publish
CSV data to S3 which this tap will consume.

## Quick start

### Install

We recommend using a virtualenv:

```bash
> virtualenv -p python3 venv
> source venv/bin/activate
> pip install tap-intacct
```

### Create the config file

The Intacct Tap requires a start_date, a bucket, and an Intacct company ID to function.

  **start_date** - an initial date for the Tap to extract data
  **bucket** - The name of an S3 bucket where the Intacct DDS is outputing data
  **company_id** - The Company ID used to login to the Intacct UI
  **path** (optional) - An optional path configured in the Intacct UI for use in the S3 bucket

### Configure your S3 Bucket

This tap uses the [boto3](https://boto3.readthedocs.io/en/latest/index.html) library for accessing S3. The [credentials](https://boto3.readthedocs.io/en/latest/guide/quickstart.html#configuration)
used by boto will also need access to the S3 bucket configured in the above config.

### Run the Tap in Discovery Mode

`tap-intacct -c config.json --discover`


---

Copyright &copy; 2018 Stitch

---

### Run the Tap with Meltano
All of the required changes to run the Tap with [Meltano](https://meltano.com/product/) are in the most recent commit of the `run with meltano` branch.

#### Functionality Limitations
The version of the Tap in the `run with meltano` branch does not have the capability to select or exclude specific streams from the source. 

#### Meltano Details
Follow [this tutorial](https://docs.meltano.com/getting-started/) for instructions on how to install and run Meltano