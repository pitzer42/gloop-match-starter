docker build -t gloop-match-starter .
docker stop match-starter
docker rm match-starter
docker run --name match-starter -d gloop-match-starter