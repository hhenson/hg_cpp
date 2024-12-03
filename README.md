# hg_cpp

A high performance implementation of the HGraph runtime.


## Development

The project is currently configured to make use of [uv](https://github.com/astral-sh/uv) for dependency management. 
Take a look at the website to see how best to install the tool.
Once you have checked out the project, you can install the project for development using the following command:

```bash
uv venv --python 3.12
```

This should be installed in the ```.venv``` folder within the project.

For users of CLion / PyCharm, you can then add the environment by selecting an existing virtual environment using
the location above.

To install the dependencies run below:

```bash
uv sync
```
