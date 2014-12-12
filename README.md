# consigliere

This is the beginnings of an app to process sosreport (and possibly SAR) data from an ElasticSearch warehouse.

## Getting the data

I was able to bulk export the sosreport indices using the [elasticdump](https://github.com/taskrabbit/elasticsearch-dump) utility; if you want to do this yourself, here's how to invoke elasticdump:

    for month in $(seq -f "%02g" 01 12) ; do
       elasticdump --all --debug --input=http://es-perf44.perf.lab.eng.bos.redhat.com:80/vos.sosreport-2014$month --output=vos.sosreport-2014$month.json
    done

For convenience, willb has also made a tarball of the extracted indices (available inside RHT; ask him for a reference).

## Preprocessing the data

Once you've extracted the JSON files to some directory, you can run the `sos-preprocess.py` script with the names of the JSON files as arguments.  Currently, this just splits each file by record type, but it might do more sophisticated preprocessing in the future.  You'll invoke it like this:

    % ../bin/sos-preprocess.py vos.sosreport-2014*.json

and the output will look something like this:

    processing vos.sosreport-201401.json...
     - writing cpuinfo records...
     - writing date records...
     - writing lsblk records...
     - writing dmidecode records...
     - writing slabinfo records...
     - writing ps records...
     - writing cmdline records...
     - writing installed-rpms records...
     - writing meminfo records...
     - writing lspci records...
     - writing lsmod records...
     - writing vmstat records...
    ...

## Launching the REPL

Run `sbt analysis/console`.  This will set up a REPL for you with some useful imports and predefined values:

    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    import org.apache.spark.rdd.RDD
    val app = new com.redhat.et.consigliere.common.ConsoleApp()
    val spark = app.context
    import app.sqlContext._

To load the data into Spark SQL `SchemaRDD`s, use the `SosReportIngest` class:

    val ingest = new com.redhat.et.consigliere.common.SosReportIngest("data", app)

`ingest` will have several (lazy) fields, one for each record type.  You can use these as you'd expect, e.g.:

    ingest.ps.registerAsTable("ps")
    sql("select count(*) from ps").collect()


   