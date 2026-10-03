FROM astrocrpublic.azurecr.io/runtime:3.3-8

ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account.json
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled