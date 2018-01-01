Run on 01-01-2018 with:

    docker run -ti -v ${PWD}:${PWD} -w ${PWD} -v /data:/data -v /var/run/docker.sock:/var/run/docker.sock --privileged gkiar/clowdr-task:01012018 task/metadata.json
