Run on 02-01-2018 with:

    docker run -ti -v /clowdata/:/clowdata/ -v /var/run/docker.sock:/var/run/docker.sock -e AWS_ACCESS_KEY_ID=AKIAINC3SILF2ITCXFUA -e AWS_SECRET_ACCESS_KEY=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 --privileged gkiar/clowdr-task:02012018 s3://clowdr-storage/tmptask/metadata.json
