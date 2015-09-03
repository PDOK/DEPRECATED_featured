# featured [![Build Status](http://sou592.so.kadaster.nl:8084/buildStatus/icon?job=featured)](http://sou592.so.kadaster.nl:8084/job/featured/)

FIXME: description

## Installation

### Windows-Installer
	Download from http://leiningen-win-installer.djpowell.net/ the installer and execute.

### Windows-manual
	Example
		Download lein.bat from https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein.bat
		set PATH %USERHOME%\.lein\bin		
	
### Linux

## Issues

### Proxy & Certificates
	Multiple applications need proxy parameters these are best set through ENV_VAR
	- LEIN_JAVA_CMD = D:\Programs\Java\jdk1.8.0_25\bin\java.exe
	- http_proxy = www-proxy.cs.kadaster.nl:8082
	- https_proxy = www-proxy.cs.kadaster.nl:8082
	- MAVEN_OPTS = -Xmx512m -XX:MaxPermSize=2048m -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true
	- JAVA_OPTS = -Dhttp.proxyHost=http://www-proxy.cs.kadaster.nl -Dhttp.proxyPort=8082 -Dhttps.proxyHost=http://www-proxy.cs.kadaster.nl -Dhttps.proxyPort=8082 -Dhttp.nonProxyHosts="localhost|127.0.0.1|*.SO.kadaster.nl|*.so.kadaster.nl|orchestration-plp.cs.kadaster.nl|10.*|fme*"
	
	Because we are behind a proxy we need to add certificates to the java keystore
	To check which certificates are need can be done through a internet browser.
		- load the page https://github.com 
		- click the https symbol in the addressbar
		- click on something like check certificate 
		- select the Certification Path, a path will be show (something like Kadaster CA, www-proxy.cs.kadaster.nl, github.com) 
		- we will need the all the certificates with the exception of the last one (github.com)
		- for each certificate select it and view the certificate, a option for downloading(/saving) the certificate will be show,... so do that!
		- when the certificates are saved they need to be added/imported to the java keystore.
		- first we need to check which keystore we are going to update, that can be done by checking the 'active' java installation
		- then the certificates can be added to the keystore
		Example
		keytool -keystore "C:\Program Files\Java\jre1.8.0_31\lib\security\cacerts" -importcert -alias kadaster_proxy -file kadaster_proxy.cer
		keytool -keystore "C:\Program Files\Java\jre1.8.0_31\lib\security\cacerts" -importcert -alias kadaster_ca -file kadaster_ca.cer

## Usage

https://github.com/technomancy/leiningen

FIXME: explanation

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

### Aanroep jar
    $ java -jar -Dprocessor_database_url=//localhost:5432/pdok -Dprocessor_database_user=pdok_owner -Dprocessor_database_password=pdok_owner -Ddata_database_url=//localhost:5432/pdok -Ddata_database_user=pdok_owner -Ddata_database_password=pdok_owner target\uberjar\featured-0.1.0-SNAPSHOT-standalone.jar -d bgt -f .test-files\new-features-single-collection-10.json

### Aanroep REST-calls
#### Verwerken json-bestanden
Op ../featured/api/process een POST uitvoeren met onderstaande request, waarbij de parameters _format_ en _callback_ optioneel zijn.
    
    {"dataset": "bgt", "file": "file://D:/Development/PDOK/featured/.test-files/change-features-single-collection-1000.json", "format":"zip", "callback": "http://localhost:3000/api/ping"}

#### Aanmaken extract-records    
Op ../featured/api/extract een POST uitvoeren met request:
  
    {"dataset": "bgt", "collection": "buurt", "extractType": "citygml", "extractVersion": "25",  "callback": "http://localhost:3000/api/ping"}
    
    
### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

For use with the REPL add the file _profiles.clj_ to your project and add the map:

       {:dev  {:env {:processor-database-url "//localhost:5432/pdok"
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
