# featured

FIXME: description

## Installation

Download from http://example.com/FIXME.

## Usage

FIXME: explanation

    $ java -jar featured-0.1.0-standalone.jar [args]

## Options

    -Dprocessor_database_url
    -Dprocessor_database_user
    -Dprocessor_database_password
    -Ddata_database_url
    -Ddata_database_user
    -Ddata_database_password

## Examples

    $ java -jar -Dprocessor_database_url=//localhost:5432/pdok -Dprocessor_database_user=pdok_owner -Dprocessor_database_password=pdok_owner -Ddata_database_url=//localhost:5432/pdok -Ddata_database_user=pdok_owner -Ddata_database_password=pdok_owner target\uberjar\featured-0.1.0-SNAPSHOT-standalone.jar .test-files\new-features-single-collection-10.json
### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

For use with the REPL add the file _profiles.clj_ to your project and add the map:

       {:dev  {:env {:processor-database_url "//localhost:5432/pdok"
              :processor-database-user "pdok_owner"
              :processor-database-password "pdok_owner"
              :data-database-url "//localhost:5432/pdok"
              :data-database-user "pdok_owner"
              :data-database-password "pdok_owner"
              }}
        :test {:env {:database-user "test-user"}}}

See also: https://github.com/weavejester/environ        

## License

Copyright © 2015 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
