# scio-db
Apache Beam SCIO repo for replicating internal INC

## Installing the Pubsub emulator
https://cloud.google.com/pubsub/docs/emulator#windows

## Starting the Pubsub emulator
gcloud beta emulators pubsub start --project=mypro

cd C:\Users\lopealb\dev\tools\emulator\python-pubsub\samples\snippets

## setting env vars
gcloud beta emulators pubsub env-init > set_vars.cmd && set_vars.cmd
set PUBSUB_PROJECT_ID=mypro
echo %PUBSUB_EMULATOR_HOST%
echo %PUBSUB_PROJECT_ID%

## init virtual env
virtualenv env
env\Scripts\activate

### requirements for virtualenv (only first time)
set PROXY=http://:@userproxy.intranet.db.com:8080
set HTTP_PROXY=%PROXY%
set HTTPS_PROXY=%PROXY%
echo %HTTP_PROXY%
env\Scripts\pip.exe install -r requirements.txt

## proxy for emulator
set PROXY=intranet.db.com:8080
set HTTP_PROXY=%PROXY%
set HTTPS_PROXY=%PROXY%
echo %HTTP_PROXY%

python publisher.py mypro create mytopic
python subscriber.py mypro create mytopic mysub
python publisher.py mypro publish mytopic
python subscriber.py mypro receive mysub
