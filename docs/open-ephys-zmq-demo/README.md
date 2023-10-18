# Open Ephys ZMQ Demo

Here's a demo / example of Pyramid with the Open Ephys ZMQ Interface plugin.

## overview

This example will read from one or more instances of the Open Ephys [ZMQ Interface plugin](https://open-ephys.github.io/gui-docs/User-Manual/Plugins/ZMQ-Interface.html).
Each reader will consume continuous data, spike, and/or ttl event data and write these into Pyramid signal and/or event buffers.
It will show plotters for the buffers to visualize data as they arrive.

This is a work in progress, a starting point as we test the OpenEphysZmqReader with the Open Ephys GUI!

## experiment YAML

The experiment YAML file [demo_experiment.yaml](demo_experiment.yaml).  I think we'll hack this up as we test.


## running it

Here's how to run Pyramid in `gui` mode, which should plot the data from Open Ephys GUI.

```
pyramid gui --trial-file demo_experiment.hdf5 --experiment demo_experiment.yaml --plot-positions plot_positions.yaml
```

The plots should update every 5 seconds or so with whatever data we've received, if any.
