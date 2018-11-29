Game of News -- back
=========

Backend application for the [Game of News application](https://github.com/FloChehab/Game-of-news).

The backend is a django app ready to be deployed on Heroku.

## Set-up

### Django server

Create a virtual env for the project and activate it:
```sh
python3 -m venv ~/envs/GON-back
source ~/envs/GON-back/bin/activate
```

Install the python dependancies inside the virutal env (after activating it):
```sh
pip install -r requirements.txt 
```

### Google Big Query

*If you don't do this, you will only be able to run the pipeline on the example dataset.*


This is a simple django application, ready to deploy on Heroku.

To add the support for live Google Big Querry, you need to [get your IDs](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-usage-python). Then, copy-paste the JSON file to the root of the project, rename it to `google_cloud_id.json` and run the following command.

```sh
export GOOGLE_APPLICATION_CREDENTIALS=`pwd`/google_cloud_id.json
```

*(The python scripts need to have access to that environement variable)*

### First run

Before your run the server for the first time, you should run this command:
```sh
python ./manage.py migrate
```

There are also diffent files that need to be set-up to handle custom datasets and query results caching:

```sh
python ./set_up.py 
```
This script should run without any errors and you should see the message `All systems are ready` at the end.

To see available parameters for this script, run:

```sh
python ./set_up.py --help
```


## Running locally

To run the server locally, use the following command:
```sh
python ./manage.py runserver 0.0.0.0:8000
```

To check if everything is working correctly, you go to [http://0.0.0.0:8000/is_gbq_active](http://0.0.0.0:8000/is_gbq_active), if something is returned, than you are good!
*(This URL will tell you if you can connect to Google Big Query)*


### Deactivating GBQ

To deactivate Google Big Querry and run only with the example dataset, you should set `OFF_LINE_PREPROCESSING = True` in the file `gdelt_proxy/settigns.py`.


### Debugging

If you want to debug server error, you should set `DEBUG = True` in the file `gdelt_proxy/settigns.py`.
