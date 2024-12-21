# MLOps-Assignment---Lead-Scoring-Case-Study

## Table of Contents
* [Problem statement](#problem-statement)
* [General Info](#general-information)
* [Technologies Used](#technologies-used)

## Problem statement
Code pro is an edtech startup that had a phenomenal seed A funding round. They used this funding to gain significant market share by campaigning aggressively. Due to aggressive campaigns the amount of junk leads increased.

A lead is generated when any person visits Code pro’s website and enters their contact details on the platform. A junk lead is when the person sharing his contact details has no interest in the product/service.

Having junk leads in the pipeline creates a lot of inefficiency in the sales process. Thus the goal of the Data Science team is to build a system that categorizes leads on their propensity to purchase Code pros course. This system will help remove the inefficiency caused by junk leads in the sales process.

## General Information
Inorder to classify any lead we will require 2 types of information about the lead. The 2 types are

1.Source of origin of the lead:  In order to better predict the propensity of the lead to purchase the course, we need some information on the source of origin of the lead. The column from ‘city_mapped’ to ‘reffered_leads’ contain the information about the source of origin of the lead

2.Interaction of the lead with the website/platform: In order to better predict the propensity of the lead to purchase the course, we need some information on what was the interaction of the lead with the platform. The column from ‘1_on_1_industry_mentorship’ to ‘whatsapp_chat_click’ store the information about the interaction the lead had with the platform.

For understanding the source of origin of the lead we have the following information

1.created_data: Timestamp at which the lead was created <br>
2.city_mapped: The city from which the lead was generated <br>
3.first_platform_c: Descirbes the platform from which the lead was generated. Examples include mobile web, Desktop, Android app etc. In our case for privacy we have mapped these to levels. <br>
4.first_utm_medium_c:  Mediums describe the type of traffic. It can be of various types a few of which are 'Organic', 'Referral', 'Paid' etc.<br>
5.first_utm_source_c: Source is where your website’s traffic comes from (individual websites, Google, Facebook etc).<br>
6.total_leads_droppped: Total number of courses a lead checked<br>
7.reffered_lead: Whether the lead was reffered our not<br>
8.1_on_1_industry_mentorship: If the user has clicked on 1-on-1 industry mentorship<br>
9.call_us_button_clicked: If the user has clicked on call us button<br>
...<br>
44.whatsapp_chat_click : If the user has clicked on whatsapp chat<br>
45.app_complete_flag: this is the column we are trying to predict. We are trying to understand whether a user will fill the application or not based on the information in the previous columns<br>


## Technologies Used
- python
- airflow
- mlflow
- sklearn
- pycaret
- pandas profiling
- pipelines

## Contact
Created by [@shootingStar10](https://github.com/shootingStar10) - feel free to contact me!
