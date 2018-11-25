Game of News -- back
=========

Backend application for the [Game of News application](https://github.com/FloChehab/Game-of-news).

## Set-up

This is a simple django application, ready to deploy on Heroku.

To add the support for live Google Big Querry, you need to [get your IDs](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-usage-python). Then, copy-paste the JSON file to the root of the project, rename it to `google_cloud_id.json` and run the following command.

```sh
export GOOGLE_APPLICATION_CREDENTIALS=`pwd`/google_cloud_id.json
```

*(The python scripts need to have access to that environement variable)*
