# Split GTFS by Agency Example

This example demonstrates how to split a GTFS zip file into multiple files, one for each specified `agency_id`, using the `gtfs_df` Ruby gem.

## Usage

```
bundle install
ruby split_by_agency.rb -i <input-gtfs.zip> --ids agency1,agency2
```

- The output files will be written to the `output/` directory, named `<agency_id>.zip`.

## Options
- `-i`, `--input PATH` — Path to the input GTFS zip file
- `--ids IDS` — Comma-separated list of agency IDs to extract

## Example

```
ruby split_by_agency.rb -i ../../spec/fixtures/sample_gtfs.zip --ids DTA,OTA
```

---

This is a port of the [original Python script](https://gist.github.com/davidmh/f51e5d93a9213e0e606a43167ff87403) using Partridge.
