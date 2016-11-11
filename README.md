# exif-index

This requires docker and docker-compose.

To run the indexer and start elasticsearch:

```
docker-compose up --build
```

To query elasticsearch:

```
curl localhost:9200/exif/<IMAGE_KEY>/1
```
