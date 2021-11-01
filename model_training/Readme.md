## Runs with docker

### Build docker command 

```
docker build -t model-train .
```

### Run the app

We need to mount the volume in case we need persist the model

```
docker run -v C:\Users\jaswa\Documents\SEDS-Capstone\model:/model -p 4000:3000 model-train
```

### Testing or usage

In order to test

Send a get request to http://127.0.0.1:4000/
