# OMniLeads ACD (Automatic Call Distribution)

This component implements all the logic involved in the processing of the voice channel.

## Build

To build an image:

```
docker buildx build --file=Dockerfile --tag=$TAG --target=run .
```

Where $TAG is the docker tag you want for image. You can check the version.txt file for the tag.

## Deploy

[OMniLeads Deploy Tool](https://gitlab.com/omnileads/omldeploytool)

## License

GPLV3