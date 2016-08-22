# Pike
pike is a SQL statement on Storm, like hive on hadoop. It translates SQL to Storm topology.


#Building from source
Pike sql parser  depends on [pike-jsqlparser](https://github.com/PPTV/Pike-JSqlParser/wiki), install pike-jsqlparser in local repo first.

Building pike is rather simple under root directory by running:

    mvn clean package -DskipTests
  
This will produce the pike-VERSION-install.tar.gz file.
