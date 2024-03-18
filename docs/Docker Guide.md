## Set up and install Docker ##

A Dockerfile is a script used to build a Docker image. It contains instructions to assemble an image by specifying a base image, adding dependencies and configuring the environment.

If you have a MacBook, you can use Docker locally to build and test your Docker image. You can download Docker Desktop for Mac [here](https://docs.docker.com/desktop/install/mac-install/)

## Testing Docker image ##

To build and test your Docker image locally, follow the steps below:

Clone your Airflow repository to a new folder on your MacBook – this guarantees that the Docker image will be built using the same code as on the Analytical Platform. You may need to create a new connection to GitHub with SSH.

Open a terminal session and navigate to the directory containing the Dockerfile using the cd command.

Build the Docker image by running:

`docker build . -t IMAGE:TAG`

where `IMAGE` is a name for the image, for example, `my-docker-image`, and `TAG` is the version number, for example, `v0.1`.

Run a Docker container created from the Docker image by running:

`docker run IMAGE:TAG`

This will run the command specified in the CMD line of the Dockerfile. This will fail if your command requires access to resources on the Analytical Platform, such as data stored in Amazon S3 unless the correct environment variables are passed to the docker container. You would need the following environment variables to ensure correct access to all the AP resources:

```
docker run \
    --env AWS_REGION=$AWS_REGION \
    --env AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
    --env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --env AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    --env AWS_SECURITY_TOKEN=$AWS_SECURITY_TOKEN \
    IMAGE:TAG
```

Oher environment variables such as PYTHON_SCRIPT_NAME or R_SCRIPT_NAME can be passed in the same way.

You can start a bash session in a running Docker container for debugging and troubleshooting purposes by running:

`docker run -it IMAGE:TAG bash`
