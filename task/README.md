Run on 02-01-2018 with:

    docker run -ti -v /clowdata/:/clowdata/ -v /var/run/docker.sock:/var/run/docker.sock -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --privileged --security-opt=seccomp:unconfined gkiar/clowdrtask:today s3://clowdr-storage/clowdrtask/2018-01-03_13:49:02-MTA10WP6D3/metadata-03.json
