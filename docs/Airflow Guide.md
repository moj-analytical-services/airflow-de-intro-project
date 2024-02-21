# Building a DAG in Airflow #

## Create a new GitHub repo using: ##

airflow-de-intro-project

While creating your own repo using the template of your choice, please ensure moj-analytical-services is designated as the owner of the repo. If your own GitHub account is left as owner, then the GitHub Action used to publish your image will fail when trying to run.

The image will have the same name as the repo name so make sure it is appropriate and reflects the pipeline you intend to run. If you are creating an example pipeline call it airflow-de-intro-project-{username}

Review the scripts/run.py. This has some code to write and copy to S3. Leave as-is if you are creating an example pipeline. Otherwise replace with your own logic.

Review the Dockerfile and the parent image and update as necessary (see Dockerfile). Leave as-is if creating the example pipeline

Review the requirements.txt file and update as necessary. Leave as-is if creating the example pipeline. See venv for more details

Create a tag and release, ensuring Target is set on the main branch. Set the tag and release to v0.0.1 if you are creating an example pipeline

Go to the Actions tab and you should see the “Build, tag, push, and make available image to pods” action running. Make sure the action passes otherwise the image will not be built

If you have permission, log in to ECR and search for your image and tag
