# Calling Pyramid via Matlab `system()`

We've been using Conda to set up a shell environment for Pyramid.
This allows Conda to obtain and manage Pyramid's Python dependencies for us.
Sometimes, like for scripting batch conversions, you might want to call Pyramid from Matlab.
This is possible but takes a little setup because Matlab configures its `system()` shell environment differently from the way Conda configures its environment.

The following steps seem to work and have been tested on a Linux machine and a macOS machine.

## Look at the Conda environment

We've been using a Conda environment called `pyramid` for Pyramid.
We can see how Conda sets up this environment and copy relevant parts to use later, from Matlab.

In a regular, non-Matlab shell check how Conda sets up the `PATH`.

```
$ conda activate pyramid
$ echo $PATH
/home/ninjaben/miniconda3/envs/pyramid/bin:/home/ninjaben/miniconda3/condabin:/home/ninjaben/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/ninjaben/.local/bin:/home/ninjaben/.local/bin
```

It looks like Conda prepends two dirs to the `PATH`: one for our `pyramid` environment and one for Conda itself.
For one machine, the overall `PATH` prefix looked like this:

```
/home/ninjaben/miniconda3/envs/pyramid/bin:/home/ninjaben/miniconda3/condabin:
```

We'll copy that and use it in Matlab, below.

## Configure the Matlab `system()` environment.

Now from the Matlab command window we can use what we copied above to configure the Matlab `system()` environment:

```
>> setenv("PATH", "/home/ninjaben/miniconda3/envs/pyramid/bin:/home/ninjaben/miniconda3/condabin:" + getenv("PATH"));
>> system("echo $PATH")
/home/ninjaben/miniconda3/envs/pyramid/bin:/home/ninjaben/miniconda3/condabin:/home/ninjaben/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/ninjaben/.local/bin
```

The `PATH` for Matlab's `system()` now has the same prefix as the Conda `PATH`.

This example is specific to one machine.  The general form would be like this:

```
>> setenv("PATH", "<<PATH prefix you copied above>>" + getenv("PATH"));
>> system("echo $PATH")
<<PATH prefix you copied above>><<original Matlab PATH>>
```

## Calling Python and Pyramid from Matlab `system()`

With that `PATH` in place, it should now be possible to call Python and Pyramid, as configured by Conda, via Matlab `system()`.
For example:

```
>> system("which python")
/home/ninjaben/miniconda3/envs/pyramid/bin/python

>> system("which pyramid")
/home/ninjaben/miniconda3/envs/pyramid/bin/pyramid

>> system("pyramid --help")
2023-10-11 15:34:59,861 [INFO] Pyramid 0.0.1
usage: pyramid [-h] [--experiment EXPERIMENT] [--subject SUBJECT] [--readers READERS [READERS ...]] [--trial-file TRIAL_FILE] [--graph-file GRAPH_FILE]
               [--plot-positions PLOT_POSITIONS] [--search-path SEARCH_PATH [SEARCH_PATH ...]] [--version]
               {gui,convert,graph}
```
