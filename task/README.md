Run on 02-01-2018 with:

    docker run -ti -v /clowdata/:/clowdata/ -v /var/run/docker.sock:/var/run/docker.sock -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} --privileged gkiar/clowdr-task:02012018 s3://clowdr-storage/clowdrtask/2018-01-03_00:31:21/861346/metadata.json
