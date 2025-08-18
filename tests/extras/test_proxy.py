# https://github.com/monokal/docker-tinyproxy
# docker run -d --name=tinyproxy -p 6666:8888 --env FilterDefaultDeny=No  monokal/tinyproxy:latest ANY
# curl -v --proxy http://127.0.0.1:6666 http://httpbingo.org/get