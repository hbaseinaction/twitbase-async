# HBase In Action: asynchbase TwitBase client

[http://www.manning.com/dimidukkhurana][0]

## Compiling the project

Code is managed by maven. Be sure to install maven on your platform
before running these commands. Also be aware that HBase is not yet
supported on the OpenJDK platform, the default JVM installed on most
modern Linux distributions. You'll want to install the Oracle (Sun)
Java 6 runtime and make sure it's configured on your `$PATH` before
you continue. Again, on Ubuntu, you may find the [`oab-java6`][1]
utility to be of use.

To build a self-contained jar:

    $ mvn package

The jar created using this by default will allow you to interact with
HBase running in standalone mode on your local machine. If you want
to interact with a remote (possibly fully distributed) HBase
deployment, you need to edit the [`HBaseClient`][2] constructor in the
source and recompile the jar.

## Using the asynchbase TwitBase client

This client uses [asynchbase][3] for communication with HBase. The
client assumes you have an existing TwitBase schema containing User
data. Run the application with this command:

    $ java -cp target/twitbase-async-1.0.0.jar \
      HBaseIA.TwitBase.AsyncUsersTool update

## License

Copyright (C) 2012 Nick Dimiduk, Amandeep Khurana

Distributed under the [Apache License, version 2.0][4], the same as HBase.

[0]: http://www.manning.com/dimidukkhurana
[1]: https://github.com/flexiondotorg/oab-java6
[2]: http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html
[3]: https://github.com/OpenTSDB/asynchbase
[4]: http://www.apache.org/licenses/LICENSE-2.0.html
