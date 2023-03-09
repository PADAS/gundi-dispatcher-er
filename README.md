# Gundi ER Dispatcher
Serverless dispatcher (CloudEvent Functions) to send observations to EarthRanger.


Supported observation types: Positions, GeoEvents and  CameraTrap Events

## Development
### Create python virtual environment with python 3.7+
```bash
python3 -m venv <venv>
```

### Activate virtual environment
```bash
. <venv>/bin/activate
```

### Install requirements
```bash
pip install -r requirements.txt
```

### Testing the serverless functions locally
You can use the [functions framework](https://cloud.google.com/functions/docs/running/function-frameworks) to run the CLoudEvent function locally.
```bash
functions-framework --signature-type=cloudevent --target=main
```
Then you can use the helper script test_local.sh to generate events.
```bash
./test_local.sh
```
