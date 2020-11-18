#!/bin/bash
cd ..
docker build --file tests/Dockerfile -t dockerspawner-test .
docker run -v /var/run/docker.sock:/var/run/docker.sock -it --rm --name dockerspawner-test dockerspawner-test
