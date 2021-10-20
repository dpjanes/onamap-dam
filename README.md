# onamap-dam
Play with DAM event importing

## Build Database

These commands (one for each destination) convert
DAM format data into something that can be pushed
into Onamap.

```sh
python dam/pei2014/Build.py > var/pei2014.yaml
```

## Loading

This puts the data into Onamap

```sh
python bin/Loader.py --destination pei2014 var/pei2014.yaml
```

