# PySpark Clean and Transform Pipeline

## Before cloning this repository:
Install and run the pyspark-elyra Docker Image from ruslanmv with the following command
```
docker run -p 8888:8888 ruslanmv/pyspark-elyra:3.1.2
```
After the container is running, it will give you 3 links. Use the last one, it starts with `http://127.0.0.1:8888/lab?token=<token>`, this will open the Apache Airflow dashboard.

Clone this repository inside the Airflow dashboard, you just have to introduce the repository's link `https://github.com/CadenaR/Clean-Transform-Pipeline`

## Using the Pipeline
For using this pipeline, you just have to open the CleanTransform.pipeline file and then press the run pipeline button.

If the following error appears:
```
Error processing operation raw (raw.py): zipfile.BadZipFile: File is not a zip file
```
It means there is a problem with the download, a solution to this is to manually download the <a href="https://drive.google.com/file/d/1vDvkbR3RkDhJAmqfCRgcdv5nVPFEXQis/view?usp=sharing" target="_blank">file</a>, then drag and drop it into the `data/inbound` folder inside the dashboard and remove the inbound layer from the pipeline file.
After this is done, you can run the pipeline again, and ther should not be a problem.
