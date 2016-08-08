# pike
pike is a SQL statement on Storm, like hive on hadoop. It translates SQL to Storm topology.

#support
If you need help using pike feel free to file an issue or contact me.

#Contributions
To help pike development you are encouraged to provide:
- feedback
- bugreports
- pull requests for new features

#Building from source
Pike sql parser  depends on [pike-jsqlparser](https://github.com/PPTV/Pike-JSqlParser/wiki), install pike-jsqlparser in local repo first.

Building pike is rather simple under root directory by running:

    mvn clean package -DskipTests
  
This will produce the pike-VERSION-install.tar.gz file.
