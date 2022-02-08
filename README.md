# Udacity Data Engineering Capstone Project

## Development

## Jupyter Notebook

Start pyspark in a docker container from the root directory in order to work with
_immigration.ipynb_.

```bash
	docker run -p 8888:8888 -v $(pwd):/home/jovyan/data jupyter/pyspark-notebook
```

## Airflow

Follow airflow docs to install airflow locally then change to the _airflow_ subdirectory
and run

```bash
	AIRFLOW_HOME=$(pwd) airflow standalone
```
