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

This library is used with many of the other libraries. It provides useful methods that can be reused among the various data pipeline libraries.

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


---

## socialmedia


---

## Setup

**Pre-requisites**

If you are using a conda environment, you can use the following commands to install required libraries. Note that depending on what set of modules you're utilizing, you may not need to install all these. 

```bash
conda install -c conda-forge pandas
pip install --upgrade google-api-python-client

```

## References
[1] R. Praski, "Google Analytics Reporting API Python Tutorial," 11 September 2015. [Online]. Available: http://www.ryanpraski.com/google-analytics-reporting-api-python-tutorial/. [Accessed 04 January 2019].