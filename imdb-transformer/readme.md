# How to run the IMDB Transformer
# ============================

## Build the Docker image
You need to build the Docker image first. This can be done by running the following command in the root directory of the db-transformer (where the Dockerfile is located):

```bash
$ docker build -t imdb-transformer .
```

You need to re-run this command every time you make changes to the Dockerfile or the requirements.txt file.

## Run the Docker container
You can run the Docker container by running the following command in the root directory of the db-transformer:

```bash
$ docker run -it --rm --name imdb-transformer imdb-transformer
```

```bash
$ docker build -t imdb-transformer . && docker run -it --rm --name imdb-transformer imdb-transformer
```