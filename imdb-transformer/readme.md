# How to run the IMDB Transformer
# ============================

## Build the Docker image
You need to build the Docker image first. This can be done by running the following command in the root directory of the db-transformer (where the Dockerfile is located):

```bash
$ docker build -t imdb-transformer .
```

## Run the Docker container
You can run the Docker container by running the following command in the root directory of the db-transformer:

```bash
$ docker run -it --rm --name imdb-transformer -v "$(pwd):/app" imdb-transformer
```

