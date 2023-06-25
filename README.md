ETL for loading weather data from [open-meteo](https://open-meteo.com/) API to database

System diagram is located in the readme of this repo: (https://github.com/kgmcquate/lake-freeze-frontend)

TODO:
- Create publicly-available job monitoring dashboard from CloudWatch logs
- Create publicly-available dashboard for weather data
- Add Data Quality step and alert
- Evaluate cost of running this in Azure instead of AWS EMR Serverless
- Run airflow on EC2 (with nat gateway) and make the UI publcly available
- Add unit tests
