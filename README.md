# Felk

A Mesos framework that uses Netflix's [Fenzo](https://github.com/Netflix/Fenzo) to make scheduling decisions.
Taken inspiration from [Elektron](https://bitbucket.org/sunybingcloud/elektron), a pluggable Mesos framework.

###Building the project
Run the following command to build an executable jar file.
```commandline
./gradlew build
```
_Note: The executable jar file would be stored in **build/libs/**._

###Running Felk
The following are required to be able to successfully run **Felk**.
* Workload configuration file
* Hostname and port of the mesos master

A workload configuration file consists of the following elements, 

- **tasks** - An array of JSON objects similar to the one in [Elektron](https://bitbucket.org/sunybingcloud/elektron).
- **fitness_calc** - The name of fitness calculator to be used. The supported fitness calculators are,
    -  _cpuBinPacker_
    -  _memBinPacker_
    -  _cpuMemBinPacker_
- **lease_offer_expiry_sec** - Expiration period for resource offers. See [leaseOfferExpirySecs](https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/TaskScheduler.java#L128).
- **lease_reject_action** - Method that Fenzo's task scheduler will call to notify of rejected offers. See [leaseRejectAction](https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/TaskScheduler.java#L113).


A sample workload configuration file is shown below.
```
{
  "tasks": [
    {
      "name": "minife",
      "cpu": 3.0,
      "ram": 4096,
      "watts": 63.141,
      "class_to_watts": {
        "A": 93.062,
        "B": 65.552,
        "C": 57.897,
        "D": 60.729
      },
      "image": "rdelvalle/minife:electron1",
      "cmd": "cd src && mpirun -np 3 miniFE.x -nx 100 -ny 100 -nz 100",
      "inst": 10
    }
  ],
  "fitness_calc": "cpuBinPacker",
  "lease_offer_expiry_sec": 120,
  "disable_shortfall_eval": true,
  "lease_reject_action": "decline"
}
```

Run the following command to execute Felk with master location as _abcd:5050_ and config file as _sample_minife_workload.json_.
```commandline
java -jar felk-1.0-SNAPSHOT.jar --master abcd:5050 --config sample_minife_workload.json
```


