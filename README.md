# featured

PDOK JSON data processor. Accepts JSON and processes it for data visualizations (Geoserver at the moment) and data extracts. 
 
## Usage

    $ java -jar featured-0.1.0-standalone.jar [args]

## Options
    -Dprocessor_database_url
    -Dprocessor_database_user
    -Dprocessor_database_password
    -Ddata_database_url
    -Ddata_database_user
    -Ddata_database_password
    -d dataset
    -f file

## Examples

### call jar
    $ java -jar -Dprocessor_database_url=//localhost:5432/pdok -Dprocessor_database_user=postgres -Dprocessor_database_password=postgres -Ddata_database_url=//localhost:5432/pdok -Ddata_database_user=postgres -Ddata_database_password=postgres featured-standalone.jar -d dataset -f path/to/file.json

### CLI
#### Process JSON
	lein run -d dataset -f path/to/file
	
#### Generate extracts
	lein run -m pdok.featured.extracts path/to/templates-dir dataset format [feature])

### Call REST api
#### Process json file
On <featured-root>/api/process POST. Parameters  _format_ and _callback_ are optional.
    
    {"dataset": "<dataset>", "file": "<URI to file>", "format":"zip/json", "callback": "<URI to callback>"}
    
#### Register templates
On <featured-root>/api/template POST:

    {"dataset":"<dataset>", "extractType":"<extracttype>", "collection":"<collection>", "partial": <bool>, "template":"<mustache template>"}

#### Create extract-records    
On <featured-root>/api/extract een POST uitvoeren met request:
  
    {"dataset": "<dataset>", "collection": "<collection>", "extractType": "<extracttype>", "extractVersion": "<version>",  "callback": "<callback-uri>"}
    
## License

Copyright © 2015 PDOK

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
