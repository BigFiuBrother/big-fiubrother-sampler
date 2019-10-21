# big-fiubrother-sampler
Big Fiubrother Sampler is a simple application to persist video chunks in a Database Store and sample the chunks into frames for further processing. Video Chunks are produced by the Big Fiubrother Camera Application and the sampled frames are sent to Big Fiubrother Face Recognition Application. 

### Prerequisites

- python3

### Install

In order to install big-fiubrother-sampler, a virtual environment is recommended. This can be achieved executing:

```
python3 -m venv big-fiubrother-sampler-venv
source big-fiubrother-sampler-venv/bin/activate
```
Now, to install all the dependencies, execute the following script: 

```
python3 -m pip install -r requirements.txt
```

### Configuration

Before running, proper configuration should be considered. Default parameters for development are stored in *config/development.yml*.

### Run

```
./run.py
```