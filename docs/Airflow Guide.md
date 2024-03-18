# Building a DAG in Airflow #

## Check ECR has pushed correctly ##

1. Review the scripts/run.py. This has some code to write and copy to S3. Leave as-is if you are creating an example pipeline. Otherwise replace with your own logic.

2. Review the Dockerfile and the parent image and update as necessary (see Docker Guide). Leave as-is if creating the example pipeline.

3. Head over to `airflow` repo and search for the example `write_to_s3.py` DAG & role  here:
   - [dev/dags/de_intro_project](https://github.com/moj-analytical-services/airflow/tree/main/environments/dev/dags/de_intro_project)
   - [dev/roles/airflow_dev_de_intro_project.yaml](https://github.com/moj-analytical-services/airflow/blob/main/environments/dev/roles/airflow_dev_de_intro_project.yaml)

4. Create a copy of these files with your own username. Keep them the same for now, located in dev aiflow.

5. Create a tag and release, ensuring Target is set on the main branch. Set the tag and release to v0.0.1 if you are creating an example pipeline

6. Go to the Actions tab and you should see the “Build, tag, push, and make available image to pods” action running. Make sure the action passes otherwise the image will not be built

7. If you have permission, log in to ECR and search for your image and tag
