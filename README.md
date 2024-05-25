# Quartz System Configuration Loader

Reads in configuration CSV file and writes out many PVs.

## Requirements

- python >= 3.11
- P4P


## CLI Loader

```sh
python -m cccr_configurer.configurer -i input.csv -o outdir
```

## PVA Server

```sh
python -m cccr_configurer.server
```

```sh
pvput FDAS:CCCR:NAME input.csv
pvput FDAS:CCCR:BODY "...contents of input.csv..."
```

Load `cccr-upload.bob` with CS-Studio/Phoebus for a nicer way to do this.
