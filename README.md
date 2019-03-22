# Data Pipelines

This repository contains useful Python wrappers for various APIs used for creating solid data pipeline.

Currently, if you're building data pipelines for any of the following, you may benefit from the code in this repository.

+ Web Analytics 
    + Google Analytics
    + Baidu Analytics
+ Social Media
    + Facebook (coming soon)
    + Twitter (coming soon)
    
---

## common library

The `common` library is used with many of the Python modules in this repository. It provides useful methods that can be reused among the various data pipeline libraries.

---
    
## webanalytics 

Web analytics data can be used by all kinds of people within your organization from data scientists to marketing to UX designers. 

**Problems**
+ The free versions of the best tools out there for web analytics may sample data when you want to analyze historic data or a very large dataset. 
+ If your organization has websites in China and the rest of the world, the web analytics data may be spread across both Google Analytics and Baidu Analytics.
+ Using only a web analytics tool(s), it is difficult (or even impossible) to integrate web analytics data with internal data for smarter and more effective analytics.

**Proposed Solution**: Create a data pipeline that extracts web analytics data, transforms it, and stores in a data lake so it's available to your organization for as long as you need.

The modules in this library will facilitate your creation of this data pipeline for the supported applications.

### googleanalytics
This library uses two GA APIs.

1) [Google Analytics Core Reporting API (v3)](https://developers.google.com/analytics/devguides/reporting/core/v3/coreDevguide)
2) [Google Analytics Core Reporting API (v4)](https://developers.google.com/analytics/devguides/reporting/core/v4/)

**Quick Start**

The main Python module in this library is GoogleApi.py. In order to use it, you must install the following pre-requisite libraries.

```bash
conda install -c conda-forge pandas
pip install --upgrade google-api-python-client
conda install -c conda-forge oauth2client
```

Furthermore, if you're using the example, you'll need to follow the instructions in the Appendix under *Setup data sinks*.

You must add the following files to use the example.

1) `config/app_config.ini`

This file stores the configuration of your application. It should have the sections and key/value pairs as demonstrated in the Appendix (if you're using the examples).

2) `client_secrets.json`

See the Appendix for more information on how to generate this file. It needs to be in the same location as GoogleApi.py (e.g. <app_dir>/googleanalytics/client_secrets.json) and should be of the following format.

```json
{
  "installed": {
    "client_id": "<your-client-id>",
    "project_id": "<your-project-id>",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://www.googleapis.com/oauth2/v3/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_secret": "<your-client-secret>",
    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob","http://localhost"]
  }
}
```

### baiduanalytics
This library uses one API.

1) [Baidu Tongji API (v1)](https://api.baidu.com/json/tongji/v1/ReportService/getData)

**Quick Start**

The main Python module in this library is BaiduApi.py. In order to use it, you must install the following pre-requisite libraries.

```bash
conda install -c conda-forge pandas
```

You must add the following files to use the example.

1) `config/app_config.ini`

This file stores the configuration of your application. It should have the sections and key/value pairs as demonstrated in the Appendix (if you're using the examples). Please see the appendix for more details on setting up this file.

---

## socialmedia

### facebook

### twitter

---

## Appendix 

### Setup data sinks

If you're using the examples, you'll need to set up the application for the place you're storing the data. 

Libraries that the examples use include the following.

```bash
# AWS 
conda install -c conda-forge boto3
pip install pyspark==2.1.3  # need a version that supports the .toPandas() method
# Parquet
conda install -c conda-forge pyarrow
conda install -c conda-forge arrow-cpp
# SQL Server
conda install -c conda-forge sqlalchemy
conda install -c conda-forge pyodbc
```

### Setup Google Analytics API

To setup an application to start extracting data from Google Analytics, you first need to have a project on [Google Cloud Platform](https://console.cloud.google.com/home). Create a project (e.g. google-analytics). Enable the Google Analytics Reporting API	and Analytics API for your project. Then in your project, do the following steps to setup your credentials [1].

1) In the left nav under APIs & Services click on Credentials.
2) Click on the OAuth consent screen just under the My Projects drop down at the top of the screen.
3) The Application name field is required so add the name “Google Analytics Python Reporting API” then click Save.
4) Click on the Create credentials blue dropdown in the center of the screen and click on OAuth client ID in the drop down.
5) Click on the Other radio button and add the name “Python Reporting API” then click create.
6) Your client ID and client secret will be shown in an overlay on the screen click ok.
7) Download the client_secret json file by clicking on the download icon on the far right.

Once you download the client secret json file, save it as client_secrets.json in the same folder as GoogleApi.py. 

The same Google user that you used to setup the project on Google Cloud Platform will also need access to the view in the Google Analytics account that you want to start gathering data from. Please ask an admin to grant access for you.  

Note: If you're company has a network firewall or extra layer of SSL certificate checking, as your IT security team to add as a SSL bypass exception.
+ \*.googleapis.com
  + This captures www.googleapis.com as well as clientservices.googleapis.com (which is what Google redirects your API calls to)
+ accounts.google.com

### app_config.ini

The application configuration file that is used in the examples for this repository is of the following format.

```ini
; app_config.ini -- Configuration for a Python application

[Logging]
logging_level=INFO
log_filename=google_analytics_python_app
max_log_dir_megabytes=2000000

[SqlDatabase]
server_name=myserver
database_name=mydb
port_number=1433
multi_subnet_failover=False
username=user1
password=password1
driver={ODBC Driver 17 for SQL Server}

[GA]
client_secrets_file=client_secrets.json

[AWS]
s3_bucket=mybucket
creds_profile_name=default
region_name=us-east-1
s3_key_root=my_app

[Spark]
app_name=my_app

[BaiduAnalytics]
username=user1
password=password1
token=mytoken
```

### Baidu Analytics token

In order to get the token needed for BaiduApi.py, you need to go to the following pages in [Baidu Analytics](https://tongji.baidu.com/web/welcome/login). Note I translated the site from Chinese to English using Google Chrome's translator.

Management > Other settings > Data export service 

Then accept and open and you should get the token.

### AWS credentials configuration

The easiest way I know of for configuring your AWS credentials is to first install the AWS command-line interface as follows.

```bash
conda install -c conda-forge awscli
# Then you should be able to execute:
aws configure
```

Enter in your credentials when prompted. You may just hit enter to leave unnecessary fields as None.

```bash
AWS Access Key ID [None]: <my_access_id>
AWS Secret Access Key [None]: <my_secret_key>
Default region name [None]:
Default output format [None]:
```

The file that gets updated here is `~/.aws/credentials`. Specifically it will add/update the `[default]` profile. You can manually edit this file and add as many profiles as you want. Just be sure to update the value for `creds_profile_name` in the `app_config.ini` file under the `[AWS]` section depending on which environment you are running your application in.

### Installing ODBC driver (Mac)

If you're on a Mac, first get Homebrew using the following command.

```bash
/usr/bin/ruby -e “$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)”
```

Then use the `brew` command to install an ODBC driver as shown in the example below. 

```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
brew install microsoft/msodbcsql/msodbcsql17
```

## References
[1] R. Praski, "Google Analytics Reporting API Python Tutorial," 11 September 2015. [Online]. Available: http://www.ryanpraski.com/google-analytics-reporting-api-python-tutorial/. [Accessed 04 January 2019].