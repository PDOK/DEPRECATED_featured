# Featured [![Build Status](https://travis-ci.org/PDOK/featured.svg?branch=master)](https://travis-ci.org/PDOK/featured)

Open-source transformation software for geodata, developed by Publieke Dienstverlening op de Kaart (PDOK).

Featured is the core application of the PDOK ETL landscape.
It accepts full and delta deliveries of geographical data in JSON format (see
<https://pdok.github.io/json-aanlevering.html> for a description of the format).
On this input data, Featured performs the following tasks:
1. Validate if the delivery is a correct continuation of the current state of the dataset.
2. On successful validation, persist the delivery in its master database.
3. Generate changelogs that allow applications further in the pipeline to easily process the changes.
 
## Usage

Featured supports a server mode and a command-line mode.
To run it locally, the following command can be used to start it in server mode:

    $ lein ring server

To use the command line locally, use:

    $ lein run [args]

or

    $ lein uberjar
    $ java -jar featured-3.0.5-standalone.jar [args]

## Options

Featured supports the following environment variables in server and command-line mode:

| Variable | Description |
|---|---|
| -Dprocessor_database_url="//localhost:5432/pdok" | |
| -Dprocessor_database_user="postgres" | |
| -Dprocessor_database_password="postgres" | |
| -Dprocessor_batch_size=10000 | Flush the incoming data to the database every N events. |
| -Dn_workers=2 | Number of parallel workers. |
| -Dcleanup_threshold=5 | Number of days to keep changelogs. |
| -Dfully_qualified_domain_name="localhost" | Domain name used in changelog URLs.* |
| -Dport=8000 | Port used in changelog URLs.* |
| -Dcontext_root= | Context root used in changelog URLs.* |
    
\*These parameters do not influence where Featured actually runs, but only make it aware of its location.
    
On the command line, the following parameters can be used apart from the environment variables mentioned above:

| Parameter | Description |
|---|---|
| -d/--dataset DATASET | Dataset name. |
| -f FILE | JSON input file. |
| --std-in | Read from std-in. |
| --changelog-dir DIRECTORY | Changelogs directory, defaults to TEMP_DIR/featured/localhost. |
| --no-timeline | Do not generate timeline tables. Use only when action is always :new. |
| --no-state | Do not generate feature stream table. Use only when action is always :new. Attention: also disables validation! |
| --disable-validation | Generate feature stream table, but disable validation. |
| -r/--replay N/COLLECTION | Replay last N events or all events from collection from persistence to projectors. |
| --single-processor | One processor for all files, reads meta data of first file only. |
| --fix FIXNAME | Execute fix. |
| --perform | Perform fix in combination with --fix. |
| -h/--help | Print help. |
| -v/--version | Print version. |

## Examples

### Command-line interface

#### Process JSON file using `lein run`
    $ lein run -d kadastralekaartv3 -f Annotatie-1.gml.json.zip

#### Replay feature stream using `java -jar`
    $ java -jar featured-3.0.5-standalone.jar -d bgt -r wegdeel

### REST API

#### Process JSON file
POST on `<featured-root>/api/process`. Parameters _format_, _callback_, _no-state_ and _no-timeline_ are optional.

    {
      "dataset": "<dataset>",
      "file": "<URI to file>",
      "format": "zip/json",
      "callback": "<URI to callback>",
      "no-state": true/false,
      "no-timeline": true/false
    }

## License

Copyright © 2015-2017 Publieke Dienstverlening op de Kaart

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
